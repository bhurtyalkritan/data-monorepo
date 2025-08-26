import os
import asyncio
import logging
from typing import List, Dict, Any, Optional
import json
import glob

import duckdb
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from openai import AsyncOpenAI
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/delta/lakehouse.db")
QDRANT_HOST = os.getenv("QDRANT_HOST", "qdrant")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://openrouter.ai/api/v1")
MODEL_NAME = os.getenv("MODEL_NAME", "meta-llama/llama-3.1-8b-instruct:free")

# Parquet paths produced by Spark
SILVER_PARQUET = "/delta/silver_latest.parquet"
GOLD_PARQUET = "/delta/gold_latest.parquet"
SILVER_PARQUET_GLOB = f"{SILVER_PARQUET}/*.parquet"
GOLD_PARQUET_GLOB = f"{GOLD_PARQUET}/*.parquet"

# FastAPI app
app = FastAPI(title="RT-Lakehouse Assistant API", version="1.0.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# Clients
duckdb_conn = None
qdrant_client = None
openai_client = None

# Background task handle
_views_refresh_task: Optional[asyncio.Task] = None

class QueryRequest(BaseModel):
    question: str
    include_chart: bool = True

class QueryResponse(BaseModel):
    sql: str
    results: List[Dict[str, Any]]
    explanation: str
    chart_config: Optional[Dict[str, Any]] = None

def setup_duckdb_views():
    """Create or refresh DuckDB views only when parquet files exist."""
    global duckdb_conn
    if duckdb_conn is None:
        return

    try:
        # Use glob to verify there are parquet files and to build the view over all parts
        if glob.glob(SILVER_PARQUET_GLOB):
            duckdb_conn.execute(
                f"""
                CREATE OR REPLACE VIEW silver_events AS 
                SELECT * FROM read_parquet('{SILVER_PARQUET_GLOB}')
                """
            )
            logger.info("DuckDB view 'silver_events' is ready")
        else:
            logger.debug("No parquet files found for silver at pattern %s", SILVER_PARQUET_GLOB)

        if glob.glob(GOLD_PARQUET_GLOB):
            duckdb_conn.execute(
                f"""
                CREATE OR REPLACE VIEW gold_kpis AS 
                SELECT * FROM read_parquet('{GOLD_PARQUET_GLOB}')
                """
            )
            logger.info("DuckDB view 'gold_kpis' is ready")
        else:
            logger.debug("No parquet files found for gold at pattern %s", GOLD_PARQUET_GLOB)

    except Exception as e:
        logger.warning(f"DuckDB view setup skipped/failed: {e}")

def init_connections():
    """Initialize database connections"""
    global duckdb_conn, qdrant_client, openai_client
    
    try:
        # DuckDB
        duckdb_conn = duckdb.connect(DUCKDB_PATH)
        logger.info("DuckDB connected successfully at %s", DUCKDB_PATH)

        # Try to set up views (will be retried in background if parquet is missing)
        setup_duckdb_views()
        
        # Qdrant
        qdrant_client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
        
        # Ensure collection exists
        try:
            qdrant_client.get_collection("sql_knowledge")
        except:
            qdrant_client.recreate_collection(
                collection_name="sql_knowledge",
                vectors_config=VectorParams(size=1536, distance=Distance.COSINE)
            )
            
            # Seed with basic knowledge
            seed_knowledge_base()
        
        logger.info("Qdrant connected successfully")
        
        # OpenAI
        if OPENAI_API_KEY:
            openai_client = AsyncOpenAI(
                api_key=OPENAI_API_KEY,
                base_url=OPENAI_BASE_URL
            )
            logger.info("OpenAI client initialized")
        else:
            logger.warning("No OpenAI API key provided")
            
    except Exception as e:
        logger.error(f"Failed to initialize connections: {e}")
        raise

def seed_knowledge_base():
    """Seed Qdrant with basic SQL knowledge"""
    knowledge_items = [
        {
            "text": "silver_events table contains raw ecommerce events with columns: event_id, user_id, product_id, event_type, price, quantity, currency, ts, ua, country",
            "metadata": {"table": "silver_events", "type": "schema"}
        },
        {
            "text": "gold_kpis table contains aggregated metrics with columns: window_start, window_end, orders, gmv, purchase_users, view_users, active_users, conversion_rate",
            "metadata": {"table": "gold_kpis", "type": "schema"}
        },
        {
            "text": "To get conversion rate trends, use gold_kpis table and look at conversion_rate column over time windows",
            "metadata": {"pattern": "conversion_rate", "type": "query_pattern"}
        },
        {
            "text": "To analyze user behavior, join silver_events by user_id and analyze event_type sequences",
            "metadata": {"pattern": "user_behavior", "type": "query_pattern"}
        },
        {
            "text": "For revenue analysis, filter silver_events where event_type='purchase' and calculate price * quantity",
            "metadata": {"pattern": "revenue", "type": "query_pattern"}
        }
    ]
    
    points = []
    for i, item in enumerate(knowledge_items):
        # In a real implementation, you'd use actual embeddings
        # For now, use dummy vectors
        vector = [0.1] * 1536  # Placeholder vector
        
        points.append(
            PointStruct(
                id=i,
                vector=vector,
                payload=item
            )
        )
    
    qdrant_client.upsert(collection_name="sql_knowledge", points=points)
    logger.info("Knowledge base seeded")

async def get_embeddings(text: str) -> List[float]:
    """Get embeddings for text"""
    if not openai_client:
        # Return dummy embedding
        return [0.1] * 1536
    
    try:
        response = await openai_client.embeddings.create(
            model="text-embedding-ada-002",
            input=text
        )
        return response.data[0].embedding
    except Exception as e:
        logger.error(f"Embedding failed: {e}")
        return [0.1] * 1536

def search_knowledge(query: str, limit: int = 3) -> List[Dict]:
    """Search knowledge base for relevant SQL patterns"""
    try:
        # Get query embedding - use dummy embedding to avoid asyncio issues
        query_vector = [0.1] * 1536  # Placeholder until we fix the async embedding
        
        # Search Qdrant
        results = qdrant_client.search(
            collection_name="sql_knowledge",
            query_vector=query_vector,
            limit=limit
        )
        
        return [hit.payload for hit in results]
    except Exception as e:
        logger.error(f"Knowledge search failed: {e}")
        return []

async def generate_sql(question: str, context: List[Dict]) -> tuple[str, str]:
    """Generate SQL query from natural language question"""
    if not openai_client:
        # Fallback SQL for common questions avoiding dependency on views
        if "conversion" in question.lower():
            return f"""
                SELECT window_start, conversion_rate 
                FROM read_parquet('{GOLD_PARQUET_GLOB}')
                ORDER BY window_start DESC 
                LIMIT 10
            """, "Showing recent conversion rates"
        else:
            return (
                f"SELECT COUNT(*) as total_events FROM read_parquet('{SILVER_PARQUET_GLOB}')",
                "Total events count"
            )
    
    # Build context
    context_text = "\n".join([item["text"] for item in context])
    
    prompt = f"""
You are a SQL expert for an ecommerce analytics lakehouse. Generate a DuckDB SQL query to answer the user's question.

Available tables:
- silver_events: Raw events (event_id, user_id, product_id, event_type, price, quantity, currency, ts, ua, country)
- gold_kpis: Aggregated KPIs (window_start, window_end, orders, gmv, purchase_users, view_users, active_users, conversion_rate)

Context from knowledge base:
{context_text}

User Question: {question}

Requirements:
1. Return valid DuckDB SQL only
2. Use proper time filtering if asking about recent data
3. Limit results to reasonable amounts (usually LIMIT 100)
4. Use appropriate aggregations

SQL:
"""

    try:
        response = await openai_client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,
            temperature=0.1
        )
        
        sql = response.choices[0].message.content.strip()
        
        # Clean up SQL (remove markdown formatting)
        if "```sql" in sql:
            sql = sql.split("```sql")[1].split("```")[0].strip()
        elif "```" in sql:
            sql = sql.split("```")[1].strip()
            
        # Generate explanation
        explanation = f"Generated SQL query to answer: {question}"
        
        return sql, explanation
        
    except Exception as e:
        logger.error(f"SQL generation failed: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to generate SQL: {e}")

def validate_sql(sql: str) -> bool:
    """Basic SQL validation"""
    sql_lower = sql.lower().strip()
    
    # Block dangerous operations
    dangerous = ["drop", "delete", "truncate", "insert", "update", "create", "alter"]
    if any(word in sql_lower for word in dangerous):
        return False
    
    # Must be a SELECT
    if not sql_lower.startswith("select"):
        return False
        
    return True

def execute_sql(sql: str) -> List[Dict[str, Any]]:
    """Execute SQL query safely"""
    if not validate_sql(sql):
        raise HTTPException(status_code=400, detail="Invalid or unsafe SQL query")
    
    try:
        cursor = duckdb_conn.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        # Convert to list of dicts
        results = []
        for row in rows:
            results.append(dict(zip(columns, row)))
            
        return results
        
    except Exception as e:
        logger.error(f"SQL execution failed: {e}")
        raise HTTPException(status_code=400, detail=f"SQL execution error: {e}")

def generate_chart_config(sql: str, results: List[Dict]) -> Optional[Dict]:
    """Generate chart configuration based on query results"""
    if not results:
        return None
    
    # Simple heuristics for chart type
    columns = list(results[0].keys())
    
    # Time series chart
    time_cols = [col for col in columns if 'time' in col.lower() or 'date' in col.lower() or 'window' in col.lower()]
    numeric_cols = [col for col in columns if isinstance(results[0].get(col), (int, float))]
    
    if time_cols and numeric_cols:
        return {
            "type": "line",
            "x": time_cols[0],
            "y": numeric_cols[0],
            "title": f"{numeric_cols[0]} over time"
        }
    elif len(numeric_cols) >= 2:
        return {
            "type": "scatter",
            "x": numeric_cols[0],
            "y": numeric_cols[1],
            "title": f"{numeric_cols[1]} vs {numeric_cols[0]}"
        }
    elif len(columns) == 2 and len(results) <= 20:
        return {
            "type": "bar",
            "x": columns[0],
            "y": columns[1],
            "title": f"{columns[1]} by {columns[0]}"
        }
    
    return None

@app.on_event("startup")
async def startup():
    """Initialize connections on startup and start background view refresh."""
    global _views_refresh_task
    init_connections()

    async def refresh_views_loop():
        while True:
            try:
                setup_duckdb_views()
            except Exception as e:
                logger.debug("View refresh loop error: %s", e)
            await asyncio.sleep(15)

    # Start loop once
    if _views_refresh_task is None:
        _views_refresh_task = asyncio.create_task(refresh_views_loop())

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "assistant-api"}

@app.post("/query", response_model=QueryResponse)
async def query_data(request: QueryRequest):
    """Main query endpoint"""
    try:
        # Search knowledge base
        context = search_knowledge(request.question)
        
        # Generate SQL
        sql, explanation = await generate_sql(request.question, context)
        
        # Execute SQL
        results = execute_sql(sql)
        
        # Generate chart config if requested
        chart_config = None
        if request.include_chart:
            chart_config = generate_chart_config(sql, results)
        
        return QueryResponse(
            sql=sql,
            results=results,
            explanation=explanation,
            chart_config=chart_config
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Query failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tables")
async def list_tables():
    """List available tables and their schemas"""
    try:
        # Get table info
        tables = duckdb_conn.execute("SHOW TABLES").fetchall()
        
        schema_info = {}
        for table in tables:
            table_name = table[0]
            columns = duckdb_conn.execute(f"DESCRIBE {table_name}").fetchall()
            schema_info[table_name] = [
                {"name": col[0], "type": col[1]} for col in columns
            ]
        
        return schema_info
        
    except Exception as e:
        logger.error(f"Failed to get table info: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/metrics")
async def get_current_metrics():
    """Get current system metrics (non-AI, parquet-only, no NOW())."""
    try:
        # Try from gold parquet first
        latest_kpis = {}
        try:
            cur = duckdb_conn.execute(
                f"""
                SELECT * FROM read_parquet('{GOLD_PARQUET_GLOB}')
                ORDER BY window_start DESC 
                LIMIT 1
                """
            )
            row = cur.fetchone()
            cols = [d[0] for d in cur.description] if cur.description else []
            latest_kpis = dict(zip(cols, row)) if row else {}
        except Exception:
            latest_kpis = {}

        # Fallback: compute latest minute from silver parquet
        if not latest_kpis:
            cur = duckdb_conn.execute(
                f"""
                WITH silver AS (
                  SELECT * FROM read_parquet('{SILVER_PARQUET_GLOB}')
                ),
                latest AS (
                  SELECT date_trunc('minute', CAST(max(ts) AS TIMESTAMP)) AS window_start FROM silver
                ),
                windowed AS (
                  SELECT s.* FROM silver s, latest l
                  WHERE CAST(s.ts AS TIMESTAMP) >= l.window_start
                    AND CAST(s.ts AS TIMESTAMP) <  l.window_start + INTERVAL '1 minute'
                ),
                agg AS (
                  SELECT
                    COUNT(*) FILTER (WHERE event_type='purchase') AS orders,
                    SUM(COALESCE(price,0)*COALESCE(quantity,0)) FILTER (WHERE event_type='purchase') AS gmv,
                    COUNT(DISTINCT CASE WHEN event_type='purchase' THEN user_id END) AS purchase_users,
                    COUNT(DISTINCT CASE WHEN event_type='page_view' THEN user_id END) AS view_users,
                    COUNT(DISTINCT user_id) AS active_users,
                    (SELECT window_start FROM latest) AS window_start
                  FROM windowed
                )
                SELECT
                  window_start,
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
                latest_kpis = dict(zip(cols, row))

        # Recent events over last hour relative to latest timestamp in silver
        cur2 = duckdb_conn.execute(
            f"""
            WITH silver AS (
              SELECT * FROM read_parquet('{SILVER_PARQUET_GLOB}')
            ),
            latest AS (
              SELECT max(ts) AS ts_max FROM silver
            )
            SELECT event_type, COUNT(*) as count
            FROM silver s, latest l
            WHERE CAST(s.ts AS TIMESTAMP) >= date_trunc('minute', CAST(l.ts_max AS TIMESTAMP)) - INTERVAL '1 hour'
            GROUP BY event_type
            """
        )
        rows2 = cur2.fetchall()
        recent_events = {row[0]: row[1] for row in rows2} if rows2 else {}

        return {"latest_kpis": latest_kpis, "recent_events": recent_events}
    except Exception as e:
        logger.warning(f"Failed to get metrics (likely parquet not ready yet): {e}")
        return {"latest_kpis": {}, "recent_events": {}}

@app.get("/trend/conversion")
async def conversion_trend(limit: int = Query(60, ge=1, le=1000)):
    """Get conversion trend; fallback to silver if gold empty."""
    try:
        # Gold first
        try:
            cur = duckdb_conn.execute(
                f"""
                SELECT window_start, conversion_rate
                FROM read_parquet('{GOLD_PARQUET_GLOB}')
                ORDER BY window_start DESC
                LIMIT {int(limit)}
                """
            )
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall()
            res = [dict(zip(cols, r)) for r in rows]
            if res:
                return {"results": res}
        except Exception:
            pass

        # Fallback from silver history
        cur = duckdb_conn.execute(
            f"""
            WITH base AS (
              SELECT date_trunc('minute', CAST(ts AS TIMESTAMP)) AS window_start,
                     CASE WHEN event_type='purchase' THEN user_id END AS purchase_user,
                     CASE WHEN event_type='page_view' THEN user_id END AS view_user
              FROM read_parquet('{SILVER_PARQUET_GLOB}')
            ),
            agg AS (
              SELECT window_start,
                     COUNT(DISTINCT purchase_user) AS purchase_users,
                     COUNT(DISTINCT view_user) AS view_users
              FROM base
              GROUP BY 1
            )
            SELECT window_start,
                   CASE WHEN view_users=0 THEN 0.0 ELSE purchase_users::DOUBLE / view_users::DOUBLE END AS conversion_rate
            FROM agg
            ORDER BY window_start DESC
            LIMIT {int(limit)}
            """
        )
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchall()
        return {"results": [dict(zip(cols, r)) for r in rows]}
    except Exception as e:
        logger.error(f"Conversion trend failed: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/trend/revenue")
async def revenue_trend(limit: int = Query(60, ge=1, le=1000)):
    """Get GMV trend; fallback to silver if gold empty."""
    try:
        try:
            cur = duckdb_conn.execute(
                f"""
                SELECT window_start, gmv
                FROM read_parquet('{GOLD_PARQUET_GLOB}')
                ORDER BY window_start DESC
                LIMIT {int(limit)}
                """
            )
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall()
            res = [dict(zip(cols, r)) for r in rows]
            if res:
                return {"results": res}
        except Exception:
            pass

        cur = duckdb_conn.execute(
            f"""
            SELECT date_trunc('minute', CAST(ts AS TIMESTAMP)) AS window_start,
                   SUM(COALESCE(price,0)*COALESCE(quantity,0)) FILTER (WHERE event_type='purchase') AS gmv
            FROM read_parquet('{SILVER_PARQUET_GLOB}')
            GROUP BY 1
            ORDER BY window_start DESC
            LIMIT {int(limit)}
            """
        )
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchall()
        return {"results": [dict(zip(cols, r)) for r in rows]}
    except Exception as e:
        logger.error(f"Revenue trend failed: {e}")
        raise HTTPException(status_code=400, detail=str(e))

if __name__ == "__main__":
    try:
        import uvicorn  # type: ignore
    except ImportError:
        uvicorn = None  # type: ignore
    if uvicorn is None:
        print("Uvicorn is not installed in this environment.")
    else:
        uvicorn.run(app, host="0.0.0.0", port=8000)
