# FastAPI Assistant Documentation

## Overview

The FastAPI Assistant (`services/assistant_api.py`) is a comprehensive analytics API with 60+ endpoints providing real-time metrics, ML features, time travel capabilities, and optional AI-powered querying.

## Core Architecture

### API Design Principles
- **Non-AI First**: Primary endpoints provide deterministic, cacheable results
- **AI Optional**: Natural language querying available but not required
- **Production Ready**: Caching, rate limiting, error handling
- **Real-Time**: Sub-second response times with streaming updates

### Technology Stack
```python
# Core dependencies
FastAPI==0.104.1        # Modern async web framework
DuckDB==0.9.2          # In-process analytical database
uvicorn==0.24.0        # ASGI server
python-multipart==0.0.6 # File upload support
```

## API Endpoints Overview

### Health & System Status
```python
@app.get("/health")
async def health():
    """Basic health check"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "uptime": time.time() - _start_time,
        "cache_stats": _cache_stats
    }

@app.get("/healthz") 
async def healthz():
    """Kubernetes-style health check with database connectivity"""
    # Tests DuckDB connection and data freshness
```

### Core Analytics Endpoints

#### Latest Metrics
```python
@app.get("/metrics")
async def get_metrics(request: Request):
    """Get latest business KPIs and recent event counts"""
    
    # Example response:
    {
        "latest_kpis": {
            "window_start": "2025-08-27T14:30:00Z",
            "window_end": "2025-08-27T14:31:00Z", 
            "orders": 42,
            "gmv": 3847.50,
            "purchase_users": 38,
            "view_users": 156,
            "active_users": 203,
            "conversion_rate": 0.244
        },
        "recent_events": {
            "page_view": 892,
            "add_to_cart": 156,
            "purchase": 42
        },
        "freshness": {
            "last_update": "2025-08-27T14:31:23Z",
            "lag_seconds": 37
        }
    }
```

#### Trend Analysis
```python
@app.get("/trend/conversion")
async def conversion_trend(
    limit: int = Query(60, description="Number of time periods"),
    request: Request = None
):
    """Get conversion rate trend over time"""
    
    # Returns time series data:
    {
        "data": [
            {
                "window_start": "2025-08-27T14:00:00Z",
                "conversion_rate": 0.234,
                "purchase_users": 35,
                "view_users": 149
            },
            # ... more data points
        ],
        "summary": {
            "avg_conversion": 0.241,
            "trend": "increasing",
            "data_points": 60
        }
    }

@app.get("/trend/revenue")
async def revenue_trend(limit: int = Query(60)):
    """Get GMV trend over time"""
    # Similar structure with revenue-focused metrics
```

### Advanced Analytics

#### Machine Learning Features
```python
@app.get("/ml/features")
async def ml_features(user_id: Optional[str] = None):
    """Get ML features for user behavior analysis"""
    
    if user_id:
        # User-specific features
        query = """
        SELECT 
            user_id,
            COUNT(*) as total_events,
            COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) as purchases,
            AVG(CASE WHEN event_type = 'purchase' THEN price * quantity END) as avg_order_value,
            COUNT(DISTINCT product_id) as unique_products_viewed,
            EXTRACT(hour FROM ts) as preferred_hour,
            -- Recency, Frequency, Monetary features
            DATE_DIFF('day', MAX(ts), NOW()) as days_since_last_activity,
            COUNT(*) / DATE_DIFF('day', MIN(ts), MAX(ts)) as avg_events_per_day
        FROM silver_events 
        WHERE user_id = ? 
        GROUP BY user_id
        """
    else:
        # Aggregate features
        query = """
        SELECT 
            AVG(session_length) as avg_session_length,
            AVG(pages_per_session) as avg_pages_per_session,
            AVG(conversion_rate) as avg_conversion_rate,
            STDDEV(gmv) as gmv_volatility
        FROM gold_kpis 
        WHERE window_start >= NOW() - INTERVAL 1 DAY
        """
```

#### Anomaly Detection
```python
@app.get("/anomaly/conversion")  
async def conversion_anomaly():
    """Detect anomalies in conversion rate"""
    
    # Statistical anomaly detection using rolling statistics
    query = """
    WITH stats AS (
        SELECT 
            window_start,
            conversion_rate,
            AVG(conversion_rate) OVER (
                ORDER BY window_start 
                ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
            ) as rolling_avg,
            STDDEV(conversion_rate) OVER (
                ORDER BY window_start 
                ROWS BETWEEN 59 PRECEDING AND CURRENT ROW  
            ) as rolling_stddev
        FROM gold_kpis
        ORDER BY window_start DESC
        LIMIT 100
    )
    SELECT *,
        ABS(conversion_rate - rolling_avg) / rolling_stddev as z_score,
        CASE 
            WHEN ABS(conversion_rate - rolling_avg) / rolling_stddev > 2 THEN 'anomaly'
            ELSE 'normal'
        END as status
    FROM stats
    WHERE rolling_stddev > 0
    """
```

#### Forecasting
```python
@app.get("/forecast/conversion")
async def conversion_forecast():
    """Simple linear trend forecasting for conversion rate"""
    
    # Linear regression for next hour prediction
    query = """
    WITH hourly_data AS (
        SELECT 
            DATE_TRUNC('hour', window_start) as hour,
            AVG(conversion_rate) as avg_conversion
        FROM gold_kpis 
        WHERE window_start >= NOW() - INTERVAL 24 HOURS
        GROUP BY DATE_TRUNC('hour', window_start)
        ORDER BY hour
    ),
    trend_calc AS (
        SELECT 
            AVG(avg_conversion) as mean_conversion,
            -- Simple linear trend calculation
            COVAR_POP(EXTRACT(epoch FROM hour), avg_conversion) / 
            VAR_POP(EXTRACT(epoch FROM hour)) as slope
        FROM hourly_data
    )
    SELECT 
        mean_conversion + slope * 3600 as forecasted_next_hour,
        slope * 3600 as hourly_trend,
        CASE 
            WHEN slope > 0 THEN 'increasing'
            WHEN slope < 0 THEN 'decreasing' 
            ELSE 'stable'
        END as trend_direction
    FROM trend_calc
    """
```

### Data Quality & Monitoring

#### Data Quality Status
```python
@app.get("/dq/status")
async def dq_status(request: Request):
    """Comprehensive data quality metrics"""
    
    # Multi-dimensional quality checks
    checks = {
        "completeness": {
            "null_rate": "SELECT AVG(CASE WHEN user_id IS NULL THEN 1.0 ELSE 0.0 END) FROM silver_events",
            "empty_rate": "SELECT AVG(CASE WHEN user_id = '' THEN 1.0 ELSE 0.0 END) FROM silver_events"
        },
        "validity": {
            "price_range": "SELECT AVG(CASE WHEN price BETWEEN 0 AND 10000 THEN 1.0 ELSE 0.0 END) FROM silver_events",
            "event_types": "SELECT AVG(CASE WHEN event_type IN ('page_view', 'add_to_cart', 'purchase') THEN 1.0 ELSE 0.0 END) FROM silver_events"
        },
        "timeliness": {
            "freshness": "SELECT EXTRACT(epoch FROM NOW() - MAX(ts)) / 60 as minutes_old FROM silver_events",
            "lag": "SELECT EXTRACT(epoch FROM NOW() - MAX(ingestion_time)) / 60 as ingestion_lag FROM silver_events"
        },
        "consistency": {
            "duplicate_rate": "SELECT (COUNT(*) - COUNT(DISTINCT event_id)) / COUNT(*) FROM silver_events",
            "schema_compliance": "SELECT 1.0 as compliance_rate"  # Placeholder for schema validation
        }
    }
```

#### Pipeline Latency Monitoring
```python
@app.get("/latency")
async def pipeline_latency(request: Request):
    """Track end-to-end pipeline latency"""
    
    # Measure latency through pipeline stages
    query = """
    WITH latency_calc AS (
        SELECT 
            DATE_TRUNC('minute', ingestion_time) as minute,
            AVG(EXTRACT(epoch FROM ingestion_time - ts)) as avg_ingestion_lag,
            MIN(ts) as first_event_ts,
            MAX(ingestion_time) as last_ingestion_ts,
            COUNT(*) as events_processed
        FROM silver_events 
        WHERE ingestion_time >= NOW() - INTERVAL 1 HOUR
        GROUP BY DATE_TRUNC('minute', ingestion_time)
        ORDER BY minute DESC
        LIMIT 10
    )
    SELECT 
        minute,
        avg_ingestion_lag,
        events_processed,
        CASE 
            WHEN avg_ingestion_lag < 5 THEN 'excellent'
            WHEN avg_ingestion_lag < 15 THEN 'good'
            WHEN avg_ingestion_lag < 30 THEN 'warning'
            ELSE 'critical'
        END as latency_status
    FROM latency_calc
    """
```

### Time Travel Integration

#### Historical Data Access
```python
@app.get("/timetravel/history")
async def time_travel_history(
    table: str = Query(..., description="Table: bronze, silver, or gold"),
    limit: int = Query(20)
):
    """Get Delta Lake version history"""
    
    table_paths = {
        "bronze": "/delta/bronze_events",
        "silver": "/delta/silver_events",
        "gold": "/delta/gold_kpis"
    }
    
    history = get_table_history(table_paths[table], limit)
    return {"table": table, "versions": len(history), "history": history}

@app.get("/timetravel/data")
async def time_travel_data(
    table: str = Query(...),
    version: Optional[int] = Query(None),
    timestamp: Optional[str] = Query(None)
):
    """Get data from specific version or timestamp"""
    # Implementation in time travel documentation
```

### Advanced Query Interface

#### Custom SQL Queries
```python
@app.post("/query")
async def custom_query(request: Request):
    """Execute custom DuckDB SQL queries with safety checks"""
    
    body = await request.json()
    sql = body.get("sql", "").strip()
    
    # Security: Only allow SELECT statements
    if not sql.upper().startswith("SELECT"):
        raise HTTPException(status_code=400, detail="Only SELECT queries allowed")
    
    # Rate limiting and validation
    ratelimit(request)
    
    try:
        result = duckdb_conn.execute(sql).fetchall()
        columns = [desc[0] for desc in duckdb_conn.description]
        
        return {
            "sql": sql,
            "columns": columns,
            "data": [dict(zip(columns, row)) for row in result],
            "row_count": len(result)
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Query error: {str(e)}")
```

#### AI-Powered Natural Language Queries (Optional)
```python
@app.post("/query/natural")
async def natural_language_query(request: Request):
    """Convert natural language to SQL and execute"""
    
    body = await request.json()
    question = body.get("question", "")
    
    # Generate SQL using AI (OpenRouter/OpenAI integration)
    sql = await generate_sql_from_question(question)
    
    # Execute with same safety checks as custom_query
    return await execute_generated_sql(sql, question)
```

## Production Features

### Caching Strategy
```python
def cached(key: str, ttl: int, loader):
    """Advanced caching with TTL and ETag support"""
    now = time.time()
    entry = _cache.get(key)
    
    # Check cache validity
    if entry and (now - entry.get("t", 0)) < ttl:
        _cache_stats["hits"] += 1
        return entry["v"], True
    
    # Load fresh data
    v = loader()
    _cache[key] = {"v": v, "t": now}
    _cache_stats["misses"] += 1
    return v, False

def etag_for(sql: Optional[str] = None) -> str:
    """Generate ETags based on query and data freshness"""
    parts = []
    if sql:
        parts.append(hashlib.sha256(sql.encode()).hexdigest()[:16])
    
    # Include data file modification times
    mtimes = [os.path.getmtime(p) for p in glob.glob("/delta/gold_latest.parquet/*.parquet")]
    parts.append(str(int(max(mtimes) if mtimes else 0)))
    
    return "W/" + "-".join(parts)
```

### Rate Limiting
```python
def ratelimit(request: Request):
    """IP-based rate limiting with sliding window"""
    ip = request.client.host if request.client else "unknown"
    bucket = _rate_buckets.setdefault(ip, deque())
    now = time.time()
    
    # Remove old requests outside window
    while bucket and bucket[0] <= now - RATE_WINDOW:
        bucket.popleft()
    
    # Check limit
    if len(bucket) >= RATE_LIMIT:
        raise HTTPException(
            status_code=429, 
            detail=f"Rate limit exceeded: {RATE_LIMIT} requests per {RATE_WINDOW} seconds"
        )
    
    bucket.append(now)
```

### Error Handling & Monitoring
```python
@app.middleware("http")
async def error_handling_middleware(request: Request, call_next):
    """Global error handling and request logging"""
    start_time = time.time()
    
    try:
        response = await call_next(request)
        process_time = time.time() - start_time
        
        # Add performance headers
        response.headers["X-Process-Time"] = str(process_time)
        response.headers["X-Cache-Hit"] = str(getattr(request, "_cache_hit", False))
        
        return response
        
    except Exception as e:
        logger.error(f"Request failed: {request.url} - {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": "Internal server error", "detail": str(e)}
        )
```

### Real-Time Streaming
```python
@app.get("/sse/metrics")
async def stream_metrics():
    """Server-Sent Events for real-time dashboard updates"""
    
    async def event_generator():
        while True:
            try:
                metrics = await load_latest_metrics()
                yield f"data: {json.dumps(metrics, default=json_serializable)}\n\n"
                await asyncio.sleep(5)  # Update every 5 seconds
            except Exception as e:
                yield f"event: error\ndata: {json.dumps({'error': str(e)})}\n\n"
                break
    
    return StreamingResponse(
        event_generator(),
        media_type="text/plain",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
    )
```

## Configuration & Environment

### Environment Variables
```bash
# Database configuration
DUCKDB_PATH=/delta/lakehouse.db

# API configuration  
RATE_LIMIT=60                    # Requests per minute
RATE_WINDOW=60                   # Rate limiting window
CONVERSION_ALERT_THRESHOLD=0.05  # 5% conversion rate alert

# AI integration (optional)
OPENROUTER_API_KEY=your_key
OPENAI_API_KEY=your_key

# Multi-tenancy (optional)
TENANT_ENFORCE=false
TENANT_COLUMN=tenant_id

# Vector database
QDRANT_URL=http://qdrant:6333
```

### Startup Configuration
```python
@app.on_event("startup")
async def startup():
    """Initialize database connections and background tasks"""
    global duckdb_conn
    
    # Initialize DuckDB with optimizations
    duckdb_conn = duckdb.connect(DUCKDB_PATH)
    duckdb_conn.execute("SET memory_limit='2GB'")
    duckdb_conn.execute("SET threads=4")
    
    # Create views for analytics
    setup_duckdb_views()
    
    # Start background refresh loop
    asyncio.create_task(refresh_views_loop())
```

## Performance Optimization

### Query Performance
- **DuckDB Optimization**: Columnar storage, vectorized execution
- **Parquet Integration**: Zero-copy data access from Delta snapshots  
- **Indexing Strategy**: Automatic column statistics and pruning
- **Memory Management**: 2GB limit with 4 threads for optimal performance

### Response Time Targets
- **Health checks**: < 10ms
- **Cached metrics**: < 50ms
- **Fresh analytics**: < 200ms
- **Complex aggregations**: < 500ms
- **Time travel queries**: < 1s

### Monitoring Metrics
- Request latency percentiles (p50, p95, p99)
- Cache hit ratios
- Database query execution times
- Error rates and types
- Active connection counts
