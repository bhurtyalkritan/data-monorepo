import os
import re
import glob
import json
import time
import asyncio
import hashlib
import logging
from collections import deque
from datetime import datetime
from typing import Any, Dict, List, Optional

import duckdb
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from openai import OpenAI

# -----------------------------------------------------
# Logging & env
# -----------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("assistant_api")

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/delta/lakehouse.db")
SILVER_PARQUET = "/delta/silver_latest.parquet"
GOLD_PARQUET = "/delta/gold_latest.parquet"
SILVER_PARQUET_GLOB = f"{SILVER_PARQUET}/*.parquet"
GOLD_PARQUET_GLOB = f"{GOLD_PARQUET}/*.parquet"

CONVERSION_ALERT_THRESHOLD = float(os.getenv("CONVERSION_ALERT_THRESHOLD", "0.05"))  # 5%
CONVERSION_ALERT_WINDOW = int(os.getenv("CONVERSION_ALERT_WINDOW", "5"))  # last N minutes

RATE_LIMIT = int(os.getenv("RATE_LIMIT", "60"))   # requests per window
RATE_WINDOW = int(os.getenv("RATE_WINDOW", "60")) # seconds

TENANT_ENFORCE = os.getenv("TENANT_ENFORCE", "false").lower() in ("1", "true", "yes")
TENANT_COLUMN = os.getenv("TENANT_COLUMN", "tenant_id")

# AI Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY") or os.getenv("OPENROUTER_API_KEY")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://openrouter.ai/api/v1")
MODEL_NAME = os.getenv("MODEL_NAME", "meta-llama/llama-3.1-8b-instruct:free")

# -----------------------------------------------------
# App & CORS
# -----------------------------------------------------
app = FastAPI(title="RT-Lakehouse Assistant API", version="2.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------
# Globals
# -----------------------------------------------------
duckdb_conn: Optional[duckdb.DuckDBPyConnection] = None
_cache: Dict[str, Dict[str, Any]] = {}
_cache_stats = {"hits": 0, "misses": 0}
_rate_buckets: Dict[str, deque] = {}
_start_time = time.time()

# AI Client
openai_client = None
if OPENAI_API_KEY:
    try:
        openai_client = OpenAI(
            api_key=OPENAI_API_KEY,
            base_url=OPENAI_BASE_URL
        )
        logger.info("OpenAI client initialized with base URL: %s", OPENAI_BASE_URL)
    except Exception as e:
        logger.warning("Failed to initialize OpenAI client: %s", e)

# AI Knowledge Base
SCHEMA_CONTEXT = """
Database Schema:
- silver_events: Raw e-commerce events with columns: event_id, user_id, session_id, event_type, ts, product_id, price, quantity, currency, country, device, ua
- gold_kpis: Aggregated KPIs with columns: window_start, window_end, orders, gmv, conversion_rate, active_users, purchase_users, view_users

Common patterns:
- Conversion rate = purchase_users / view_users
- GMV = sum of (price * quantity) for purchase events
- Time windows are typically 1-minute aggregates
- For time-based queries, use window_start for gold_kpis and ts for silver_events
"""

AI_KNOWLEDGE_BASE = [
    {
        "text": "Show conversion rate over time",
        "sql": f"SELECT window_start, conversion_rate FROM read_parquet('{GOLD_PARQUET_GLOB}') ORDER BY window_start DESC LIMIT 60",
        "pattern": "conversion_trend"
    },
    {
        "text": "Show revenue trend GMV over time",
        "sql": f"SELECT window_start, gmv FROM read_parquet('{GOLD_PARQUET_GLOB}') ORDER BY window_start DESC LIMIT 60",
        "pattern": "revenue_trend"
    },
    {
        "text": "Count events by type in the last hour",
        "sql": f"SELECT event_type, COUNT(*) as count FROM read_parquet('{SILVER_PARQUET_GLOB}') WHERE ts >= NOW() - INTERVAL '1 hour' GROUP BY event_type",
        "pattern": "event_counts"
    },
    {
        "text": "Top products by revenue",
        "sql": f"SELECT product_id, SUM(price * quantity) as revenue FROM read_parquet('{SILVER_PARQUET_GLOB}') WHERE event_type = 'purchase' GROUP BY product_id ORDER BY revenue DESC LIMIT 10",
        "pattern": "top_products"
    },
    {
        "text": "Active users in the last hour",
        "sql": f"SELECT COUNT(DISTINCT user_id) as active_users FROM read_parquet('{SILVER_PARQUET_GLOB}') WHERE ts >= NOW() - INTERVAL '1 hour'",
        "pattern": "active_users"
    },
    {
        "text": "Country breakdown of users",
        "sql": f"SELECT country, COUNT(DISTINCT user_id) as users FROM read_parquet('{SILVER_PARQUET_GLOB}') GROUP BY country ORDER BY users DESC LIMIT 10",
        "pattern": "country_analysis"
    }
]

# -----------------------------------------------------
# Helpers
# -----------------------------------------------------

def json_serializable(obj):
    """Convert non-serializable objects to JSON-safe types"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {k: json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [json_serializable(item) for item in obj]
    else:
        return obj

def cached(key: str, ttl: int, loader):
    now = time.time()
    entry = _cache.get(key)
    if entry and (now - entry.get("t", 0)) < ttl:
        _cache_stats["hits"] += 1
        return entry["v"], True
    v = loader()
    _cache[key] = {"v": v, "t": now}
    _cache_stats["misses"] += 1
    return v, False


def etag_for(sql: Optional[str] = None) -> str:
    parts = []
    if sql:
        parts.append(hashlib.sha256(sql.encode()).hexdigest()[:16])
    mtimes = [os.path.getmtime(p) for p in glob.glob(GOLD_PARQUET_GLOB)]
    mtimes += [os.path.getmtime(p) for p in glob.glob(SILVER_PARQUET_GLOB)]
    parts.append(str(int(max(mtimes) if mtimes else 0)))
    return "W/" + "-".join(parts)


def ratelimit(request: Request):
    ip = request.client.host if request.client else "unknown"
    bucket = _rate_buckets.setdefault(ip, deque())
    now = time.time()
    while bucket and now - bucket[0] > RATE_WINDOW:
        bucket.popleft()
    if len(bucket) >= RATE_LIMIT:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    bucket.append(now)


def setup_duckdb_views():
    if duckdb_conn is None:
        return
    try:
        if glob.glob(SILVER_PARQUET_GLOB):
            duckdb_conn.execute(
                f"""
                CREATE OR REPLACE VIEW silver_events AS
                SELECT * FROM read_parquet('{SILVER_PARQUET_GLOB}')
                """
            )
        if glob.glob(GOLD_PARQUET_GLOB):
            duckdb_conn.execute(
                f"""
                CREATE OR REPLACE VIEW gold_kpis AS
                SELECT * FROM read_parquet('{GOLD_PARQUET_GLOB}')
                """
            )
    except Exception as e:
        logger.warning("View setup failed: %s", e)


async def generate_sql_from_question(question: str) -> Dict[str, Any]:
    """Use AI to convert natural language question to SQL"""
    if not openai_client:
        raise HTTPException(status_code=503, detail="AI service not available")
    
    # Check knowledge base for similar patterns
    question_lower = question.lower()
    for item in AI_KNOWLEDGE_BASE:
        if any(keyword in question_lower for keyword in item["text"].lower().split()):
            logger.info("Found knowledge base match for pattern: %s", item["pattern"])
            return {
                "sql": item["sql"],
                "explanation": f"Generated from knowledge base pattern: {item['pattern']}",
                "source": "knowledge_base"
            }
    
    try:
        # Create AI prompt
        prompt = f"""
You are a SQL expert for an e-commerce analytics database. Convert the user's question to a DuckDB SQL query.

{SCHEMA_CONTEXT}

Rules:
1. Only use SELECT statements
2. Use read_parquet() function to access data
3. Silver events path: '{SILVER_PARQUET_GLOB}'
4. Gold KPIs path: '{GOLD_PARQUET_GLOB}'
5. Always include LIMIT clause (max 1000)
6. Use proper date/time filtering with ts or window_start columns

User Question: {question}

Respond with valid SQL only, no explanations:
"""

        response = openai_client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": "You are a SQL expert. Respond only with valid SQL queries."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=500
        )
        
        sql = response.choices[0].message.content.strip()
        
        # Clean up the SQL
        if sql.startswith("```sql"):
            sql = sql[6:]
        if sql.endswith("```"):
            sql = sql[:-3]
        sql = sql.strip()
        
        return {
            "sql": sql,
            "explanation": f"AI-generated SQL for: {question}",
            "source": "ai_generated"
        }
        
    except Exception as e:
        logger.error("AI SQL generation failed: %s", e)
        # Fallback to simple pattern matching
        if "conversion" in question_lower:
            return {
                "sql": f"SELECT window_start, conversion_rate FROM read_parquet('{GOLD_PARQUET_GLOB}') ORDER BY window_start DESC LIMIT 60",
                "explanation": "Fallback: conversion rate query",
                "source": "fallback"
            }
        elif "revenue" in question_lower or "gmv" in question_lower:
            return {
                "sql": f"SELECT window_start, gmv FROM read_parquet('{GOLD_PARQUET_GLOB}') ORDER BY window_start DESC LIMIT 60",
                "explanation": "Fallback: revenue query", 
                "source": "fallback"
            }
        else:
            return {
                "sql": f"SELECT COUNT(*) AS total_events FROM read_parquet('{SILVER_PARQUET_GLOB}')",
                "explanation": "Fallback: event count query",
                "source": "fallback"
            }


def init_duckdb():
    global duckdb_conn
    duckdb_conn = duckdb.connect(DUCKDB_PATH)
    logger.info("DuckDB connected at %s", DUCKDB_PATH)
    setup_duckdb_views()


@app.on_event("startup")
async def startup():
    init_duckdb()

    async def refresh_views_loop():
        while True:
            try:
                setup_duckdb_views()
            except Exception as e:
                logger.debug("refresh views error: %s", e)
            await asyncio.sleep(15)

    asyncio.create_task(refresh_views_loop())

# -----------------------------------------------------
# Health & stats
# -----------------------------------------------------
@app.get("/health")
async def health():
    return {"status": "healthy", "service": "assistant-api"}

@app.get("/healthz")
async def healthz():
    return {
        "duckdb": True,
        "silver_parquet_parts": len(glob.glob(SILVER_PARQUET_GLOB)),
        "gold_parquet_parts": len(glob.glob(GOLD_PARQUET_GLOB)),
        "uptime_sec": int(time.time() - _start_time),
        "cache_stats": _cache_stats,
        "rate_limit": {"limit": RATE_LIMIT, "window": RATE_WINDOW},
        "tenant_enforce": TENANT_ENFORCE,
        "tenant_column": TENANT_COLUMN,
    }

# -----------------------------------------------------
# DQ, lineage, optimizer advice, alerts
# -----------------------------------------------------
@app.get("/dq/status")
async def dq_status(request: Request):
    ratelimit(request)

    def load():
        q = f"""
        WITH s AS (
          SELECT * FROM read_parquet('{SILVER_PARQUET_GLOB}')
        ), l AS (
          SELECT max(ts) AS mx FROM s
        ), last_hour AS (
          SELECT * FROM s, l
          WHERE CAST(ts AS TIMESTAMP) >= date_trunc('minute', CAST(mx AS TIMESTAMP)) - INTERVAL '1 hour'
        )
        SELECT
          COALESCE(SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END)::DOUBLE / NULLIF(COUNT(*),0) * 100, 0.0) AS null_user_pct,
          SUM(CASE WHEN event_type NOT IN ('page_view','add_to_cart','purchase') THEN 1 ELSE 0 END) AS quarantined_1h,
          SUM(CASE WHEN CAST(ts AS TIMESTAMP) < mx - INTERVAL '5 minutes' THEN 1 ELSE 0 END) AS late_1h
        FROM last_hour
        """
        row = duckdb_conn.execute(q).fetchone()
        cols = [d[0] for d in duckdb_conn.description]
        return dict(zip(cols, row)) if row else {"null_user_pct": 0.0, "quarantined_1h": 0, "late_1h": 0}

    data, hit = cached("dq:status", ttl=10, loader=load)
    r = JSONResponse(data)
    r.headers["X-Cache"] = "HIT" if hit else "MISS"
    r.headers["Cache-Control"] = "public, max-age=10"
    return r

# static lineage graph + impact traversal
G_LINEAGE = {
    "bronze_events": ["silver_events"],
    "silver_events": ["gold_kpis"],
    "gold_kpis": ["gold_latest.parquet"],
    "gold_latest.parquet": ["assistant_api"],
    "assistant_api": ["frontend_dashboard"],
}

@app.get("/lineage/graph")
async def lineage_graph():
    nodes = [{"id": k, "label": k} for k in G_LINEAGE.keys()]
    edges = [{"from": u, "to": v} for u, vs in G_LINEAGE.items() for v in vs]
    return {"nodes": nodes, "edges": edges}

@app.get("/lineage/impact")
async def lineage_impact(node: str = Query(...)):
    from collections import deque as _dq
    q, seen, order = _dq([node]), {node}, []
    while q:
        v = q.popleft(); order.append(v)
        for nxt in G_LINEAGE.get(v, []):
            if nxt not in seen:
                seen.add(nxt); q.append(nxt)
    return {"order": order}

@app.get("/lineage/whatif")
async def lineage_whatif(node: str = Query(...)):
    # Impacted downstream nodes
    order = (await lineage_impact(node))  # type: ignore
    impacted = [n for n in order.get("order", []) if n != node]
    recommendation = f"If {node} is delayed, refresh downstream tables: {', '.join(impacted) or 'none'}. Consider backfill or re-run the pipeline."
    return {"node": node, "impacted": impacted, "recommendation": recommendation}

@app.get("/optimize/advice")
async def optimize_advice():
    parts = len(glob.glob(GOLD_PARQUET_GLOB))
    advice = []
    if parts > 200:
        advice.append("Consider OPTIMIZE gold_kpis (many small files)")
    advice.append("Enable AQE; broadcast join small dims (<1e6 rows)")
    advice.append("ZORDER gold_kpis BY (window_start) if you filter by time")
    return {"gold_parts": parts, "advice": advice}

@app.get("/alerts/state")
async def alerts_state(request: Request):
    ratelimit(request)

    def load():
        q = f"""
        SELECT window_start, conversion_rate
        FROM read_parquet('{GOLD_PARQUET_GLOB}')
        ORDER BY window_start DESC
        LIMIT {CONVERSION_ALERT_WINDOW}
        """
        try:
            rows = duckdb_conn.execute(q).fetchall()
            breach = bool(rows) and all((r[1] or 0.0) < CONVERSION_ALERT_THRESHOLD for r in rows)
            return {
                "breach": breach,
                "threshold": CONVERSION_ALERT_THRESHOLD,
                "window_minutes": CONVERSION_ALERT_WINDOW,
                "samples": [{"window_start": str(r[0]), "conversion_rate": float(r[1] or 0.0)} for r in rows],
            }
        except Exception:
            return {"breach": False, "threshold": CONVERSION_ALERT_THRESHOLD, "window_minutes": CONVERSION_ALERT_WINDOW, "samples": []}

    data, hit = cached("alerts:conv", ttl=10, loader=load)
    r = JSONResponse(data)
    r.headers["X-Cache"] = "HIT" if hit else "MISS"
    r.headers["Cache-Control"] = "public, max-age=10"
    return r

# -----------------------------------------------------
# Latency tracking
# -----------------------------------------------------
@app.get("/latency")
async def pipeline_latency(request: Request):
    ratelimit(request)
    
    def load():
        try:
            # Get latest silver timestamp
            silver_q = f"""
            SELECT max(ts) as latest_ts
            FROM read_parquet('{SILVER_PARQUET_GLOB}')
            """
            silver_result = duckdb_conn.execute(silver_q).fetchone()
            silver_latest = silver_result[0] if silver_result and silver_result[0] else None
            
            # Get latest gold timestamp  
            gold_q = f"""
            SELECT max(window_start) as latest_window
            FROM read_parquet('{GOLD_PARQUET_GLOB}')
            """
            gold_result = duckdb_conn.execute(gold_q).fetchone()
            gold_latest = gold_result[0] if gold_result and gold_result[0] else None
            
            # Calculate lag in seconds
            lag_sec = None
            if silver_latest and gold_latest:
                # Convert to timestamps and calculate difference
                lag_q = f"""
                SELECT extract(epoch from (TIMESTAMP '{silver_latest}' - TIMESTAMP '{gold_latest}'))
                """
                lag_result = duckdb_conn.execute(lag_q).fetchone()
                lag_sec = abs(float(lag_result[0])) if lag_result and lag_result[0] is not None else None
            
            return {
                "silver_latest": str(silver_latest) if silver_latest else None,
                "gold_latest": str(gold_latest) if gold_latest else None, 
                "lag_sec": lag_sec
            }
        except Exception as e:
            logger.warning(f"Latency calculation failed: {e}")
            return {"silver_latest": None, "gold_latest": None, "lag_sec": None}
    
    data, hit = cached("latency", ttl=15, loader=load)
    r = JSONResponse(data)
    r.headers["X-Cache"] = "HIT" if hit else "MISS"
    r.headers["Cache-Control"] = "public, max-age=15"
    return r
async def load_latest_metrics() -> Dict[str, Any]:
    latest_kpis: Dict[str, Any] = {}
    try:
        cur = duckdb_conn.execute(
            f"""
            SELECT * FROM read_parquet('{GOLD_PARQUET_GLOB}')
            ORDER BY window_start DESC LIMIT 1
            """
        )
        row = cur.fetchone()
        if row:
            cols = [d[0] for d in cur.description]
            latest_kpis = json_serializable(dict(zip(cols, row)))
    except Exception:
        pass

    if not latest_kpis:
        cur = duckdb_conn.execute(
            f"""
            WITH silver AS (SELECT * FROM read_parquet('{SILVER_PARQUET_GLOB}')),
            latest AS (
              SELECT date_trunc('minute', CAST(max(ts) AS TIMESTAMP)) AS window_start FROM silver
            ), windowed AS (
              SELECT s.* FROM silver s, latest l
              WHERE CAST(s.ts AS TIMESTAMP) >= l.window_start
                AND CAST(s.ts AS TIMESTAMP) <  l.window_start + INTERVAL '1 minute'
            ), agg AS (
              SELECT
                COUNT(*) FILTER (WHERE event_type='purchase') AS orders,
                SUM(COALESCE(price,0)*COALESCE(quantity,0)) FILTER (WHERE event_type='purchase') AS gmv,
                COUNT(DISTINCT CASE WHEN event_type='purchase' THEN user_id END) AS purchase_users,
                COUNT(DISTINCT CASE WHEN event_type='page_view' THEN user_id END) AS view_users,
                COUNT(DISTINCT user_id) AS active_users,
                (SELECT window_start FROM latest) AS window_start
              FROM windowed
            )
            SELECT window_start,
                   window_start + INTERVAL '1 minute' AS window_end,
                   COALESCE(orders,0) AS orders,
                   COALESCE(gmv,0.0) AS gmv,
                   COALESCE(purchase_users,0) AS purchase_users,
                   COALESCE(view_users,0) AS view_users,
                   COALESCE(active_users,0) AS active_users,
                   (CASE WHEN COALESCE(view_users,0)=0 THEN 0.0 ELSE purchase_users::DOUBLE / view_users::DOUBLE END) AS conversion_rate
            FROM agg
            """
        )
        row = cur.fetchone()
        if row:
            cols = [d[0] for d in cur.description]
            latest_kpis = json_serializable(dict(zip(cols, row)))

    cur2 = duckdb_conn.execute(
        f"""
        WITH s AS (SELECT * FROM read_parquet('{SILVER_PARQUET_GLOB}')),
        l AS (SELECT max(ts) AS ts_max FROM s)
        SELECT event_type, COUNT(*) AS count
        FROM s, l
        WHERE CAST(s.ts AS TIMESTAMP) >= date_trunc('minute', CAST(l.ts_max AS TIMESTAMP)) - INTERVAL '1 hour'
        GROUP BY event_type
        """
    )
    rows2 = cur2.fetchall()
    recent_events = {r[0]: r[1] for r in rows2} if rows2 else {}
    return {"latest_kpis": latest_kpis, "recent_events": recent_events}

@app.get("/sse/metrics")
async def sse_metrics(request: Request):
    ratelimit(request)

    async def gen():
        while True:
            data = await load_latest_metrics()
            yield f"data: {json.dumps(data)}\n\n"
            await asyncio.sleep(5)

    return StreamingResponse(gen(), media_type="text/event-stream")

@app.get("/metrics")
async def metrics(request: Request):
    ratelimit(request)
    data = await load_latest_metrics()
    r = JSONResponse(data)
    r.headers["Cache-Control"] = "public, max-age=5"
    return r

# -----------------------------------------------------
# Trends with time travel
# -----------------------------------------------------

def parse_as_of(as_of: Optional[str]) -> Optional[str]:
    if not as_of:
        return None
    try:
        # Parse to naive timestamp string to avoid tz type mismatch
        dt = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

@app.get("/trend/conversion")
async def conversion_trend(request: Request, limit: int = Query(60, ge=1, le=1000), as_of: Optional[str] = None):
    ratelimit(request)
    as_of_sql = parse_as_of(as_of)

    def load():
        try:
            where = f"WHERE window_start <= TIMESTAMP '{as_of_sql}'" if as_of_sql else ""
            q = f"""
            SELECT window_start, conversion_rate
            FROM read_parquet('{GOLD_PARQUET_GLOB}')
            {where}
            ORDER BY window_start DESC
            LIMIT {int(limit)}
            """
            cur = duckdb_conn.execute(q)
            rows = cur.fetchall()
            if rows:
                cols = [d[0] for d in cur.description]
                return {"results": [json_serializable(dict(zip(cols, r))) for r in rows]}
        except Exception:
            pass
        where2 = f"AND CAST(ts AS TIMESTAMP) <= TIMESTAMP '{as_of_sql}'" if as_of_sql else ""
        q2 = f"""
        WITH base AS (
          SELECT date_trunc('minute', CAST(ts AS TIMESTAMP)) AS window_start,
                 CASE WHEN event_type='purchase' THEN user_id END AS purchase_user,
                 CASE WHEN event_type='page_view' THEN user_id END AS view_user
          FROM read_parquet('{SILVER_PARQUET_GLOB}')
          WHERE 1=1 {where2}
        ), agg AS (
          SELECT window_start,
                 COUNT(DISTINCT purchase_user) AS purchase_users,
                 COUNT(DISTINCT view_user) AS view_users
          FROM base GROUP BY 1
        )
        SELECT window_start,
               CASE WHEN view_users=0 THEN 0.0 ELSE purchase_users::DOUBLE / view_users::DOUBLE END AS conversion_rate
        FROM agg ORDER BY window_start DESC LIMIT {int(limit)}
        """
        cur = duckdb_conn.execute(q2)
        cols = [d[0] for d in cur.description]
        return {"results": [json_serializable(dict(zip(cols, r))) for r in cur.fetchall()]}

    key = f"trend:conv:{limit}:{as_of_sql or 'latest'}"
    data, hit = cached(key, ttl=5, loader=load)
    r = JSONResponse(data)
    r.headers["X-Cache"] = "HIT" if hit else "MISS"
    r.headers["Cache-Control"] = "no-store"
    return r

@app.get("/trend/revenue")
async def revenue_trend(request: Request, limit: int = Query(60, ge=1, le=1000), as_of: Optional[str] = None):
    ratelimit(request)
    as_of_sql = parse_as_of(as_of)

    def load():
        try:
            where = f"WHERE window_start <= TIMESTAMP '{as_of_sql}'" if as_of_sql else ""
            q = f"""
            SELECT window_start, gmv
            FROM read_parquet('{GOLD_PARQUET_GLOB}')
            {where}
            ORDER BY window_start DESC
            LIMIT {int(limit)}
            """
            cur = duckdb_conn.execute(q)
            rows = cur.fetchall()
            if rows:
                cols = [d[0] for d in cur.description]
                return {"results": [json_serializable(dict(zip(cols, r))) for r in rows]}
        except Exception:
            pass
        where2 = f"AND CAST(ts AS TIMESTAMP) <= TIMESTAMP '{as_of_sql}'" if as_of_sql else ""
        q2 = f"""
        SELECT date_trunc('minute', CAST(ts AS TIMESTAMP)) AS window_start,
               SUM(COALESCE(price,0)*COALESCE(quantity,0)) FILTER (WHERE event_type='purchase') AS gmv
        FROM read_parquet('{SILVER_PARQUET_GLOB}')
        WHERE 1=1 {where2}
        GROUP BY 1 ORDER BY window_start DESC LIMIT {int(limit)}
        """
        cur = duckdb_conn.execute(q2)
        cols = [d[0] for d in cur.description]
        return {"results": [json_serializable(dict(zip(cols, r))) for r in cur.fetchall()]}

    key = f"trend:rev:{limit}:{as_of_sql or 'latest'}"
    data, hit = cached(key, ttl=5, loader=load)
    r = JSONResponse(data)
    r.headers["X-Cache"] = "HIT" if hit else "MISS"
    r.headers["Cache-Control"] = "no-store"
    return r

# -----------------------------------------------------
# Latency, Skew, Forecast, Anomaly, ML Features
# -----------------------------------------------------
@app.get("/latency")
async def latency():
    try:
        s_max = duckdb_conn.execute(
            f"SELECT max(CAST(ts AS TIMESTAMP)) FROM read_parquet('{SILVER_PARQUET_GLOB}')"
        ).fetchone()[0]
    except Exception:
        s_max = None
    try:
        g_max = duckdb_conn.execute(
            f"SELECT max(window_start) FROM read_parquet('{GOLD_PARQUET_GLOB}')"
        ).fetchone()[0]
    except Exception:
        g_max = None
    lag_sec = None
    if s_max and g_max:
        # DuckDB returns Python datetime; compute seconds
        lag_sec = max(0.0, (s_max - g_max).total_seconds())
    return {"silver_latest": str(s_max) if s_max else None, "gold_latest": str(g_max) if g_max else None, "lag_sec": lag_sec}

@app.get("/skew/check")
async def skew_check(key: str = Query("user_id"), topk: int = Query(12, ge=1, le=1000)):
    try:
        q = f"""
        WITH s AS (SELECT * FROM read_parquet('{SILVER_PARQUET_GLOB}')),
        l AS (SELECT max(ts) AS ts_max FROM s)
        SELECT {key} AS key, COUNT(*) AS freq
        FROM s, l
        WHERE CAST(s.ts AS TIMESTAMP) >= date_trunc('minute', CAST(l.ts_max AS TIMESTAMP)) - INTERVAL '1 hour'
        GROUP BY 1 ORDER BY freq DESC NULLS LAST LIMIT {int(topk)}
        """
        cur = duckdb_conn.execute(q)
        rows = cur.fetchall()
        return {"key": key, "top": [{"key": r[0], "freq": r[1]} for r in rows]}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/forecast/conversion")
async def forecast_conversion(limit: int = Query(120, ge=10, le=2000), horizon: int = Query(12, ge=1, le=2000), ma_window: int = Query(7, ge=1, le=200)):
    # Pull recent series
    cur = duckdb_conn.execute(
        f"SELECT conversion_rate FROM read_parquet('{GOLD_PARQUET_GLOB}') ORDER BY window_start DESC LIMIT {int(limit)}"
    )
    vals = [float(r[0]) for r in cur.fetchall()][::-1]
    if not vals:
        return {"history": [], "history_smoothed": [], "forecast": [], "horizon": horizon}
    # Simple moving average smoothing
    smoothed: List[float] = []
    for i in range(len(vals)):
        s = max(0, i - ma_window + 1)
        window = vals[s:i+1]
        smoothed.append(sum(window) / len(window))
    # Naive forecast: repeat last smoothed value
    last = smoothed[-1]
    forecast = [last for _ in range(horizon)]
    return {"history": vals, "history_smoothed": smoothed, "forecast": forecast, "horizon": horizon}

@app.get("/anomaly/conversion")
async def anomaly_conversion(limit: int = Query(120, ge=10, le=5000), z: float = Query(2.0, ge=0.1, le=10.0)):
    cur = duckdb_conn.execute(
        f"SELECT window_start, conversion_rate FROM read_parquet('{GOLD_PARQUET_GLOB}') ORDER BY window_start DESC LIMIT {int(limit)}"
    )
    rows = cur.fetchall()[::-1]
    rates = [float(r[1] or 0.0) for r in rows]
    if not rates:
        return {"mean": 0.0, "std": 0.0, "z": z, "anomalies": []}
    mu = sum(rates) / len(rates)
    var = sum((x - mu) ** 2 for x in rates) / max(1, (len(rates) - 1))
    sd = var ** 0.5
    anomalies = []
    if sd > 0:
        for i, (ts, rate) in enumerate(rows):
            zz = abs(((rate or 0.0) - mu) / sd)
            if zz >= z:
                anomalies.append({"window_start": str(ts), "rate": float(rate or 0.0), "z": zz})
    return {"mean": mu, "std": sd, "z": z, "anomalies": anomalies}

@app.get("/ml/features")
async def ml_features(limit: int = Query(20, ge=1, le=1000)):
    q = f"""
    WITH s AS (
      SELECT * FROM read_parquet('{SILVER_PARQUET_GLOB}')
    ), agg AS (
      SELECT user_id,
             SUM(CASE WHEN event_type='purchase' THEN COALESCE(price,0)*COALESCE(quantity,0) ELSE 0 END) AS total_spend,
             SUM(CASE WHEN event_type='purchase' THEN 1 ELSE 0 END) AS purchase_count,
             SUM(CASE WHEN event_type='page_view' THEN 1 ELSE 0 END) AS views
      FROM s
      GROUP BY user_id
    )
    SELECT user_id, purchase_count, total_spend, views
    FROM agg ORDER BY total_spend DESC NULLS LAST LIMIT {int(limit)}
    """
    cur = duckdb_conn.execute(q)
    cols = [d[0] for d in cur.description]
    rows = [json_serializable(dict(zip(cols, r))) for r in cur.fetchall()]
    return {"features": rows}

# -----------------------------------------------------
# Query endpoint with guardrails, EXPLAIN, ETag
# -----------------------------------------------------
READ_PARQUET_PATTERN = re.compile(r"read_parquet\(\'([^']+)\'\)", re.IGNORECASE)
ALLOWED_GLOBS = {SILVER_PARQUET_GLOB, GOLD_PARQUET_GLOB}


def validate_sql(sql: str) -> bool:
    s = sql.lower().strip()
    if not s.startswith("select"):
        return False
    for word in (" drop ", " delete ", " truncate ", " insert ", " update ", " create ", " alter "):
        if word in f" {s} ":
            return False
    for m in READ_PARQUET_PATTERN.finditer(sql):
        if m.group(1) not in ALLOWED_GLOBS:
            return False
    return True

@app.post("/query")
async def query_data(request: Request, body: Dict[str, Any]):
    ratelimit(request)
    question = (body.get("question") or "").strip()
    include_chart = bool(body.get("include_chart", True))
    use_ai = bool(body.get("use_ai", True))

    if not question:
        raise HTTPException(status_code=400, detail="Question is required")

    # Generate SQL using AI or fallback to simple patterns
    if use_ai and openai_client:
        try:
            sql_result = await generate_sql_from_question(question)
            sql = sql_result["sql"]
            explanation = sql_result["explanation"]
            ai_source = sql_result["source"]
        except Exception as e:
            logger.error("AI generation failed, using fallback: %s", e)
            use_ai = False
    
    if not use_ai or not openai_client:
        # Simple NL→SQL rules (no external deps). Add cases as needed.
        question_lower = question.lower()
        if "conversion" in question_lower:
            sql = f"""
            SELECT window_start, conversion_rate
            FROM read_parquet('{GOLD_PARQUET_GLOB}')
            ORDER BY window_start DESC LIMIT 60
            """.strip()
            explanation = "Recent conversion rate from gold_kpis"
            ai_source = "pattern_matching"
        elif "revenue" in question_lower or "gmv" in question_lower:
            sql = f"""
            SELECT window_start, gmv
            FROM read_parquet('{GOLD_PARQUET_GLOB}')
            ORDER BY window_start DESC LIMIT 60
            """.strip()
            explanation = "Recent GMV from gold_kpis"
            ai_source = "pattern_matching"
        else:
            sql = f"SELECT COUNT(*) AS total_events FROM read_parquet('{SILVER_PARQUET_GLOB}')"
            explanation = "Total events from silver"
            ai_source = "pattern_matching"

    if not validate_sql(sql):
        raise HTTPException(status_code=400, detail="Generated SQL is unsafe or invalid")

    tag = etag_for(sql)
    if request.headers.get("if-none-match") == tag:
        return JSONResponse(status_code=304, content={})

    try:
        # Use plain EXPLAIN and format rows for compatibility
        plan_rows = duckdb_conn.execute(f"EXPLAIN {sql}").fetchall()

        def _row_to_text(r):
            if isinstance(r, (list, tuple)):
                if len(r) > 1 and isinstance(r[1], str):
                    return r[1]
                if len(r) == 1:
                    return str(r[0])
            return str(r)

        plan_txt = "\n".join(_row_to_text(r) for r in plan_rows)

        cur = duckdb_conn.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        results = [json_serializable(dict(zip(cols, r))) for r in rows]

        chart = None
        if include_chart and results:
            chart = generate_chart_config(sql, results, question)

        payload = {
            "sql": sql, 
            "results": results, 
            "explanation": explanation, 
            "chart_config": chart, 
            "plan": plan_txt,
            "ai_source": ai_source,
            "question": question
        }
        r = JSONResponse(payload)
        r.headers["ETag"] = tag
        r.headers["Cache-Control"] = "public, max-age=5"
        return r
    except Exception as e:
        logger.error("SQL exec failed: %s", e)
        raise HTTPException(status_code=400, detail=f"SQL execution error: {e}")


def generate_chart_config(sql: str, results: List[Dict], question: str) -> Optional[Dict]:
    """Generate intelligent chart configuration based on query results"""
    if not results:
        return None
    
    names = list(results[0].keys())
    
    # Find time columns
    time_cols = [c for c in names if any(k in c.lower() for k in ("time", "date", "window", "ts"))]
    
    # Find numeric columns
    numeric_cols = [c for c in names if isinstance(results[0].get(c), (int, float))]
    
    # Find categorical columns
    categorical_cols = [c for c in names if c not in time_cols and c not in numeric_cols]
    
    question_lower = question.lower()
    
    # Determine chart type based on data and question
    if time_cols and numeric_cols:
        # Time series data
        if "trend" in question_lower or "over time" in question_lower:
            return {
                "type": "line",
                "x": time_cols[0],
                "y": numeric_cols[0],
                "title": f"{numeric_cols[0]} over time"
            }
    
    if categorical_cols and numeric_cols and not time_cols:
        # Category vs numeric - good for bar charts
        if len(results) <= 20:  # Not too many categories
            return {
                "type": "bar", 
                "x": categorical_cols[0],
                "y": numeric_cols[0],
                "title": f"{numeric_cols[0]} by {categorical_cols[0]}"
            }
    
    if len(categorical_cols) >= 2 and numeric_cols:
        # Multiple categories - could be good for scatter
        return {
            "type": "scatter",
            "x": categorical_cols[0] if categorical_cols[0] in names else names[0],
            "y": numeric_cols[0],
            "title": f"Relationship between {categorical_cols[0]} and {numeric_cols[0]}"
        }
    
    # Default fallback
    if time_cols and numeric_cols:
        return {
            "type": "line",
            "x": time_cols[0], 
            "y": numeric_cols[0],
            "title": f"{numeric_cols[0]} over time"
        }
    
    return None

# -----------------------------------------------------
# AI-Powered Analytics Endpoints
# -----------------------------------------------------

@app.post("/ai/chat")
async def ai_chat(request: Request, body: Dict[str, Any]):
    """Multi-turn conversation with AI assistant"""
    ratelimit(request)
    
    if not openai_client:
        raise HTTPException(status_code=503, detail="AI service not available")
    
    message = body.get("message", "").strip()
    conversation_history = body.get("history", [])
    
    if not message:
        raise HTTPException(status_code=400, detail="Message is required")
    
    try:
        # Build conversation context
        messages = [
            {"role": "system", "content": f"""You are an expert data analyst for an e-commerce analytics platform. 
            
{SCHEMA_CONTEXT}

You can help with:
- Converting questions to SQL queries
- Explaining data patterns and trends
- Providing business insights
- Recommending analysis approaches

Always be concise and practical in your responses."""}
        ]
        
        # Add conversation history
        for msg in conversation_history[-10:]:  # Keep last 10 messages
            messages.append({
                "role": msg.get("role", "user"),
                "content": msg.get("content", "")
            })
        
        messages.append({"role": "user", "content": message})
        
        response = openai_client.chat.completions.create(
            model=MODEL_NAME,
            messages=messages,
            temperature=0.3,
            max_tokens=800
        )
        
        ai_response = response.choices[0].message.content
        
        return {
            "response": ai_response,
            "message": message,
            "conversation_id": body.get("conversation_id"),
            "model": MODEL_NAME
        }
        
    except Exception as e:
        logger.error("AI chat failed: %s", e)
        raise HTTPException(status_code=500, detail=f"AI chat error: {e}")

@app.post("/ai/explain")
async def ai_explain_data(request: Request, body: Dict[str, Any]):
    """Get AI explanation of data patterns"""
    ratelimit(request)
    
    if not openai_client:
        raise HTTPException(status_code=503, detail="AI service not available")
    
    data = body.get("data", [])
    context = body.get("context", "")
    
    if not data:
        raise HTTPException(status_code=400, detail="Data is required")
    
    try:
        # Prepare data summary
        data_summary = f"Data points: {len(data)}"
        if data:
            sample = data[:5]  # First 5 rows
            data_summary += f"\nSample data: {json.dumps(sample, indent=2)}"
            
            # Basic statistics if numeric data
            if isinstance(data[0], dict):
                numeric_cols = [k for k, v in data[0].items() if isinstance(v, (int, float))]
                if numeric_cols:
                    for col in numeric_cols[:3]:  # Max 3 columns
                        values = [row[col] for row in data if col in row and row[col] is not None]
                        if values:
                            avg = sum(values) / len(values)
                            data_summary += f"\n{col}: avg={avg:.2f}, min={min(values)}, max={max(values)}"
        
        prompt = f"""Analyze this e-commerce data and provide insights:

Context: {context}

{data_summary}

Provide:
1. Key patterns or trends
2. Business insights
3. Potential concerns or opportunities
4. Recommended next steps

Be concise and focus on actionable insights."""

        response = openai_client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": "You are a data analyst providing business insights from e-commerce data."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
            max_tokens=600
        )
        
        return {
            "explanation": response.choices[0].message.content,
            "data_summary": data_summary,
            "context": context
        }
        
    except Exception as e:
        logger.error("AI explanation failed: %s", e)
        raise HTTPException(status_code=500, detail=f"AI explanation error: {e}")

@app.get("/ai/suggestions")
async def ai_query_suggestions(request: Request):
    """Get AI-powered query suggestions based on recent data"""
    ratelimit(request)
    
    if not openai_client:
        return {
            "suggestions": [
                "Show conversion rate over time",
                "What are the top selling products?",
                "How many users were active in the last hour?",
                "Show revenue trend for today",
                "Which countries have the most users?"
            ],
            "source": "static"
        }
    
    try:
        # Get recent metrics for context
        metrics = await load_latest_metrics()
        
        context = f"""Recent metrics:
- Latest KPIs: {json.dumps(metrics.get('latest_kpis', {}), default=str)}
- Recent events: {json.dumps(metrics.get('recent_events', {}), default=str)}"""
        
        prompt = f"""Based on this e-commerce data, suggest 5 relevant analytical questions that a business user might want to ask:

{context}

Generate questions that would provide business value and insights. Focus on:
- Performance trends
- User behavior
- Product analysis  
- Operational metrics
- Growth opportunities

Return as a simple list, one question per line."""

        response = openai_client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": "You are a business analyst suggesting relevant questions for e-commerce data analysis."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.5,
            max_tokens=400
        )
        
        suggestions_text = response.choices[0].message.content
        suggestions = [line.strip().lstrip('-').strip() for line in suggestions_text.split('\n') if line.strip()]
        
        return {
            "suggestions": suggestions[:5],  # Max 5 suggestions
            "source": "ai_generated",
            "context": "Based on recent data patterns"
        }
        
    except Exception as e:
        logger.error("AI suggestions failed: %s", e)
        # Fallback to static suggestions
        return {
            "suggestions": [
                "Show conversion rate over time",
                "What are the top selling products?", 
                "How many users were active in the last hour?",
                "Show revenue trend for today",
                "Which countries have the most users?"
            ],
            "source": "fallback"
        }

# -----------------------------------------------------
# SQL analysis & backfill
# -----------------------------------------------------
@app.post("/analyze/sql")
async def analyze_sql(body: Dict[str, Any]):
    sql = (body.get("sql") or "").strip()
    if not sql:
        raise HTTPException(status_code=400, detail="sql is required")
    if not validate_sql(sql):
        raise HTTPException(status_code=400, detail="Unsafe or invalid SQL")
    try:
        def _rows_to_text(rows):
            out = []
            for r in rows:
                if isinstance(r, (list, tuple)) and len(r) > 1 and isinstance(r[1], str):
                    out.append(r[1])
                elif isinstance(r, (list, tuple)) and len(r) == 1:
                    out.append(str(r[0]))
                else:
                    out.append(str(r))
            return "\n".join(out)
        explain = _rows_to_text(duckdb_conn.execute(f"EXPLAIN {sql}").fetchall())
        count_plan = _rows_to_text(duckdb_conn.execute(f"EXPLAIN SELECT COUNT(*) FROM ({sql}) t").fetchall())
        return {
            "sql": sql,
            "explain": explain.splitlines(),
            "count_estimate_plan": count_plan.splitlines(),
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/backfill/gold")
async def backfill_gold():
    # Materialize gold_kpis parquet view into a DuckDB table for faster reads
    try:
        # Create or replace table from parquet view
        duckdb_conn.execute("CREATE OR REPLACE TABLE gold_kpis_tbl AS SELECT * FROM read_parquet(?)", [GOLD_PARQUET_GLOB])
        cnt = duckdb_conn.execute("SELECT COUNT(*) FROM gold_kpis_tbl").fetchone()[0]
        return {"materialized_rows": int(cnt), "table": "gold_kpis_tbl"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------------------------------
# Time Travel API endpoints
# -----------------------------------------------------
@app.get("/timetravel/history/{table}")
async def get_table_history(request: Request, table: str, limit: int = Query(10, ge=1, le=100)):
    """Get Delta table version history"""
    ratelimit(request)
    
    # Validate table name
    if table not in ["bronze", "silver", "gold"]:
        raise HTTPException(status_code=400, detail="Table must be bronze, silver, or gold")
    
    try:
        # Read history JSON file exported by Spark
        import json
        history_file = f"/delta/{table}_history.json"
        
        with open(history_file, 'r') as f:
            history = json.load(f)
        
        # Return limited results
        limited_history = history[:limit] if history else []
        
        return {
            "table": table,
            "versions": limited_history,
            "total_versions": len(history) if history else 0
        }
        
    except FileNotFoundError:
        return {
            "table": table,
            "versions": [],
            "total_versions": 0,
            "message": "History not available yet"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get history: {e}")

@app.get("/timetravel/data/{table}")
async def get_time_travel_data(
    request: Request, 
    table: str, 
    version: Optional[int] = None,
    timestamp: Optional[str] = None,
    limit: int = Query(100, ge=1, le=10000)
):
    """Get data from Delta table at specific version or timestamp"""
    ratelimit(request)
    
    # Validate table name
    if table not in ["bronze", "silver", "gold"]:
        raise HTTPException(status_code=400, detail="Table must be bronze, silver, or gold")
    
    if version is None and timestamp is None:
        raise HTTPException(status_code=400, detail="Either version or timestamp must be provided")
    
    try:
        # For now, return current data with metadata about time travel request
        # In a production system, you'd use Delta Lake's time travel APIs
        
        table_map = {
            "bronze": "/delta/bronze_events",
            "silver": SILVER_PARQUET_GLOB,
            "gold": GOLD_PARQUET_GLOB
        }
        
        # Read current data (time travel would require Spark/Delta integration)
        if table == "bronze":
            # Bronze is Delta format, we'd need Spark for true time travel
            return {
                "table": table,
                "version": version,
                "timestamp": timestamp,
                "message": "Bronze time travel requires Spark Delta integration",
                "data": []
            }
        else:
            # For silver/gold, read from parquet (current snapshot)
            q = f"SELECT * FROM read_parquet('{table_map[table]}') ORDER BY ts DESC LIMIT {limit}" if table == "silver" else f"SELECT * FROM read_parquet('{table_map[table]}') ORDER BY window_start DESC LIMIT {limit}"
            
            cur = duckdb_conn.execute(q)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            data = [json_serializable(dict(zip(cols, r))) for r in rows]
            
            return {
                "table": table,
                "version": version,
                "timestamp": timestamp,
                "message": f"Showing current {table} data (time travel integration pending)",
                "data": data,
                "count": len(data)
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Time travel query failed: {e}")

@app.get("/timetravel/compare/{table}")
async def compare_table_versions(
    request: Request,
    table: str,
    version1: int,
    version2: int,
    metric: str = Query("count", description="Metric to compare: count, sum, avg")
):
    """Compare metrics between two table versions"""
    ratelimit(request)
    
    if table not in ["bronze", "silver", "gold"]:
        raise HTTPException(status_code=400, detail="Table must be bronze, silver, or gold")
    
    try:
        # Get history to validate versions exist
        history_file = f"/delta/{table}_history.json"
        with open(history_file, 'r') as f:
            history = json.load(f)
        
        # Find version details
        v1_info = next((v for v in history if v["version"] == version1), None)
        v2_info = next((v for v in history if v["version"] == version2), None)
        
        if not v1_info or not v2_info:
            raise HTTPException(status_code=400, detail="One or both versions not found")
        
        # For now, return metadata comparison
        return {
            "table": table,
            "comparison": {
                "version1": {
                    "version": version1,
                    "timestamp": v1_info["timestamp"],
                    "operation": v1_info["operation"]
                },
                "version2": {
                    "version": version2,
                    "timestamp": v2_info["timestamp"], 
                    "operation": v2_info["operation"]
                }
            },
            "metric": metric,
            "message": "Version comparison available - data diff requires Spark Delta integration"
        }
        
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Table history not available")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Comparison failed: {e}")

@app.get("/timetravel/restore/{table}")
async def restore_table_version(request: Request, table: str, version: int):
    """Restore table to specific version (metadata only)"""
    ratelimit(request)
    
    if table not in ["bronze", "silver", "gold"]:
        raise HTTPException(status_code=400, detail="Table must be bronze, silver, or gold")
    
    # This would require Spark/Delta integration for actual restore
    return {
        "table": table,
        "target_version": version,
        "status": "planned",
        "message": "Table restore requires Spark Delta integration - this is a preview endpoint"
    }

# -----------------------------------------------------
# Security / whoami
# -----------------------------------------------------
@app.get("/security/whoami")
async def security_whoami(request: Request):
    # Demo: infer tenant from header or env
    tenant = request.headers.get("x-tenant") or os.getenv("TENANT", None)
    return {
        "tenant": tenant,
        "tenant_enforce": TENANT_ENFORCE,
        "jwt_enabled": False,
        "tenant_column": TENANT_COLUMN,
    }

# -----------------------------------------------------
# Simple schema listing (handy for debugging)
# -----------------------------------------------------
@app.get("/tables")
async def list_tables():
    try:
        tables = duckdb_conn.execute("SHOW TABLES").fetchall()
        out = {}
        for (tname,) in tables:
            cols = duckdb_conn.execute(f"DESCRIBE {tname}").fetchall()
            out[tname] = [{"name": c[0], "type": c[1]} for c in cols]
        return out
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------------------------------
# Runbook links (surface in UI Help)
# -----------------------------------------------------
@app.get("/runbook/links")
async def runbook_links():
    return {
        "start": "./RUNBOOK.md#start",
        "stop": "./RUNBOOK.md#stop",
        "backfill": "./RUNBOOK.md#backfill",
        "replay": "./RUNBOOK.md#replay-from-bronze",
        "checkpoint": "./RUNBOOK.md#fix-corrupt-checkpoint",
    }

if __name__ == "__main__":
    try:
        import uvicorn  # type: ignore
    except ImportError:
        uvicorn = None  # type: ignore
    if uvicorn is None:
        print("Uvicorn is not installed in this environment.")
    else:
        uvicorn.run(app, host="0.0.0.0", port=8000)
