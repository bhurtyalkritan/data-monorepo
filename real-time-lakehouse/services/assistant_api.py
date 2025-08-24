import os, re, json, time
from typing import List, Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import requests
from databricks import sql as dbsql

# Env
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
MODEL_ENDPOINT_URL = os.getenv("MODEL_ENDPOINT_URL")
MODEL_TOKEN = os.getenv("MODEL_TOKEN", DATABRICKS_TOKEN)
VS_ENDPOINT = os.getenv("VS_ENDPOINT")
VS_INDEX = os.getenv("VS_INDEX")
CATALOG = os.getenv("CATALOG", "demo")
SCHEMA = os.getenv("SCHEMA", "ecommerce_rt")
DB = f"{CATALOG}.{SCHEMA}"
GOLD = f"{DB}.gold_kpis"
V_TIMESERIES = f"{DB}.v_kpi_timeseries"
LOG_TABLE = f"{DB}.assistant_logs"

for k, v in {
    'DATABRICKS_HOST': DATABRICKS_HOST,
    'DATABRICKS_HTTP_PATH': DATABRICKS_HTTP_PATH,
    'DATABRICKS_TOKEN': DATABRICKS_TOKEN,
    'MODEL_ENDPOINT_URL': MODEL_ENDPOINT_URL,
    'VS_ENDPOINT': VS_ENDPOINT,
    'VS_INDEX': VS_INDEX,
}.items():
    if not v:
        raise RuntimeError(f"Missing env var: {k}")

DDL_DML_PATTERN = re.compile(r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|MERGE|GRANT|REVOKE)\b", re.I)
MULTI_STMT_PATTERN = re.compile(r";\s*\S")
TIME_FILTER_PATTERN = re.compile(r"\b(window_start|ts)\b", re.I)
ALLOWED_SCHEMAS = {DB}
RAW_LIMIT = 200
DEFAULT_TIME_WINDOW_MIN = 60*24

SYSTEM = (
    f"You are a Databricks SQL expert. Prefer {GOLD} and {V_TIMESERIES}. "
    "SELECT-only; add LIMIT for raw rows; avoid CROSS JOINs; output JSON with sql, explanation, chart."
)

class AskBody(BaseModel):
    question: str
    minutes: Optional[int] = None

app = FastAPI(title="Lakehouse Assistant API")

def open_conn():
    return dbsql.connect(server_hostname=DATABRICKS_HOST, http_path=DATABRICKS_HTTP_PATH, access_token=DATABRICKS_TOKEN)

def vs_query(query: str, k: int = 6):
    url = f"{VS_ENDPOINT}/indexes/{VS_INDEX}/query"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}
    r = requests.post(url, headers=headers, json={"query": query, "k": k}, timeout=15)
    r.raise_for_status()
    res = r.json().get("results", [])
    chunks = []
    for rj in res:
        meta = rj.get("metadata", {})
        title = meta.get("title") or meta.get("table_name") or "doc"
        body = rj.get("text") or rj.get("body") or ""
        chunks.append(f"[{title}]\n{body}")
    return "\n\n".join(chunks)

def call_llm(messages: List[dict]):
    headers = {"Authorization": f"Bearer {MODEL_TOKEN}", "Content-Type": "application/json"}
    body = {"messages": messages, "max_tokens": 512, "temperature": 0.1}
    r = requests.post(MODEL_ENDPOINT_URL, headers=headers, json=body, timeout=30)
    r.raise_for_status()
    data = r.json()
    content = data.get("choices", [{}])[0].get("message", {}).get("content") or data
    return content

def enforce_whitelist(sql_text: str) -> str:
    if DDL_DML_PATTERN.search(sql_text):
        raise HTTPException(400, "Only SELECT allowed")
    if MULTI_STMT_PATTERN.search(sql_text):
        raise HTTPException(400, "Multiple statements not allowed")
    # Ensure catalog/schema present
    for token in re.findall(r"\bfrom\s+([\w\.]+)|\bjoin\s+([\w\.]+)", sql_text, re.I):
        tbl = next((t for t in token if t), None)
        if not tbl:
            continue
        parts = tbl.split('.')
        if len(parts) == 1:
            sql_text = re.sub(fr"\b{tbl}\b", f"{DB}.{tbl}", sql_text)
        elif len(parts) == 2 and parts[0] != CATALOG:
            sql_text = sql_text.replace(tbl, f"{CATALOG}.{tbl}")
    return sql_text

def ensure_limits(sql_text: str) -> str:
    if re.search(r"\bgroup\s+by\b", sql_text, re.I):
        return sql_text
    if re.search(r"\blimit\b", sql_text, re.I):
        return sql_text
    return sql_text.rstrip() + f"\nLIMIT {RAW_LIMIT}"

def ensure_time_filter(sql_text: str, minutes: Optional[int]) -> str:
    if minutes is None and TIME_FILTER_PATTERN.search(sql_text) and re.search(r"now\s*\(\)\s*-\s*INTERVAL", sql_text, re.I):
        return sql_text
    window = minutes if minutes is not None else DEFAULT_TIME_WINDOW_MIN
    col = "window_start" if "window_start" in sql_text else "ts"
    if re.search(r"\bwhere\b", sql_text, re.I):
        return re.sub(r"\bwhere\b", f"WHERE {col} >= now() - INTERVAL {window} minutes AND ", sql_text, flags=re.I)
    return sql_text + f"\nWHERE {col} >= now() - INTERVAL {window} minutes"

def run_sql(sql_text: str) -> dict:
    with open_conn() as conn:
        cur = conn.cursor()
        cur.execute("USE CATALOG " + CATALOG)
        cur.execute("USE SCHEMA " + SCHEMA)
        cur.execute("EXPLAIN " + sql_text)
        cur.execute(sql_text)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return {"columns": cols, "rows": rows}

@app.get("/health")
async def health():
    return {"ok": True}

@app.post("/ask")
async def ask(body: AskBody):
    t0 = time.time()
    ctx = vs_query(body.question)
    prompt = f"Context:\n{ctx}\n\nQuestion: {body.question}\nReturn JSON only."
    content = call_llm([
        {"role":"system","content": SYSTEM},
        {"role":"user","content": prompt}
    ])
    try:
        plan = json.loads(content)
    except Exception:
        m = re.search(r"\{[\s\S]*\}", content)
        if not m:
            raise HTTPException(500, "LLM did not return JSON")
        plan = json.loads(m.group(0))

    sql_text = plan.get("sql", "").strip()
    if not sql_text.lower().startswith("select"):
        raise HTTPException(400, "Generated SQL must be SELECT")

    sql_text = enforce_whitelist(sql_text)
    sql_text = ensure_time_filter(sql_text, body.minutes)
    sql_text = ensure_limits(sql_text)

    try:
        res = run_sql(sql_text)
        latency = int((time.time() - t0) * 1000)
        row_count = len(res["rows"]) if res and "rows" in res else 0
        # Log
        try:
            with open_conn() as conn:
                cur = conn.cursor()
                cur.execute("USE CATALOG " + CATALOG)
                cur.execute("USE SCHEMA " + SCHEMA)
                cur.execute(
                    f"CREATE TABLE IF NOT EXISTS {LOG_TABLE} (event_time TIMESTAMP, user_question STRING, plan_json STRING, sql_text STRING, row_count INT, latency_ms BIGINT, error STRING) USING DELTA"
                )
                cur.execute(
                    f"INSERT INTO {LOG_TABLE} VALUES (current_timestamp(), ?, ?, ?, ?, ?, ?)",
                    (body.question, json.dumps(plan), sql_text, row_count, latency, None)
                )
        except Exception:
            pass
        return {"plan": plan, "result": res, "latency_ms": latency}
    except Exception as e:
        # attempt a single repair
        err = str(e)
        repair_prompt = f"SQL had error: {err}\nOriginal SQL: {sql_text}\nFix and return JSON only with updated sql."
        content2 = call_llm([
            {"role":"system","content": SYSTEM},
            {"role":"user","content": repair_prompt}
        ])
        try:
            fixed = json.loads(content2)
        except Exception:
            m = re.search(r"\{[\s\S]*\}", content2)
            if not m:
                raise HTTPException(500, err)
            fixed = json.loads(m.group(0))
        sql_text2 = enforce_whitelist(ensure_limits(ensure_time_filter(fixed.get("sql", sql_text), body.minutes)))
        res = run_sql(sql_text2)
        latency = int((time.time() - t0) * 1000)
        try:
            with open_conn() as conn:
                cur = conn.cursor()
                cur.execute("USE CATALOG " + CATALOG)
                cur.execute("USE SCHEMA " + SCHEMA)
                cur.execute(
                    f"CREATE TABLE IF NOT EXISTS {LOG_TABLE} (event_time TIMESTAMP, user_question STRING, plan_json STRING, sql_text STRING, row_count INT, latency_ms BIGINT, error STRING) USING DELTA"
                )
                cur.execute(
                    f"INSERT INTO {LOG_TABLE} VALUES (current_timestamp(), ?, ?, ?, ?, ?, ?)",
                    (body.question, json.dumps(fixed), sql_text2, len(res['rows']), latency, err)
                )
        except Exception:
            pass
        return {"plan": fixed, "result": res, "latency_ms": latency}

# To run locally:
# uvicorn services.assistant_api:app --reload --port 8000
