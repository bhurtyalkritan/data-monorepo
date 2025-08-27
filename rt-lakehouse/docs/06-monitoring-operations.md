# Monitoring & Operations Documentation

## Overview

The RT-Lakehouse includes comprehensive monitoring capabilities through Streamlit dashboards, health checks, and operational metrics to ensure system reliability and performance visibility.

## Monitoring Architecture

### Components Overview
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Streamlit     │    │   FastAPI       │    │   Spark         │
│   Dashboard     │◀──▶│   Health        │◀──▶│   Metrics       │
│                 │    │   Endpoints     │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   System        │    │   API           │    │   Pipeline      │
│   Metrics       │    │   Monitoring    │    │   Monitoring    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Streamlit Monitoring Dashboard

### Main Application (`services/monitoring/app.py`)

#### System Overview Page
```python
import streamlit as st
import pandas as pd
import plotly.express as px
import requests
from datetime import datetime, timedelta

def main_dashboard():
    """Main monitoring dashboard with system overview"""
    
    st.set_page_config(
        page_title="RT-Lakehouse Monitoring",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("🏠 RT-Lakehouse System Monitor")
    st.markdown("Real-time monitoring for the entire lakehouse pipeline")
    
    # Service status overview
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        service_status("Kafka", check_kafka_health())
    with col2:
        service_status("Spark", check_spark_health())
    with col3:
        service_status("API", check_api_health())
    with col4:
        service_status("Frontend", check_frontend_health())
    
    # Key metrics dashboard
    show_key_metrics()
    show_pipeline_health()
    show_data_quality_metrics()

def service_status(service_name: str, is_healthy: bool):
    """Display service status with colored indicators"""
    status_color = "🟢" if is_healthy else "🔴"
    status_text = "Healthy" if is_healthy else "Down"
    
    st.metric(
        label=f"{status_color} {service_name}",
        value=status_text,
        delta="OK" if is_healthy else "ALERT"
    )
```

#### Real-Time Metrics Display
```python
def show_key_metrics():
    """Display real-time business metrics"""
    st.subheader("📈 Real-Time Business Metrics")
    
    try:
        # Fetch latest metrics from API
        response = requests.get(f"{API_URL}/metrics", timeout=5)
        metrics = response.json()
        
        if "latest_kpis" in metrics:
            kpis = metrics["latest_kpis"]
            
            # Create metrics columns
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    "Orders",
                    value=kpis.get("orders", 0),
                    delta=calculate_delta("orders", kpis.get("orders", 0))
                )
            
            with col2:
                gmv = kpis.get("gmv", 0)
                st.metric(
                    "Revenue (GMV)",
                    value=f"${gmv:,.2f}",
                    delta=f"${calculate_delta('gmv', gmv):,.2f}"
                )
            
            with col3:
                conversion = kpis.get("conversion_rate", 0) * 100
                st.metric(
                    "Conversion Rate",
                    value=f"{conversion:.2f}%",
                    delta=f"{calculate_delta('conversion_rate', conversion):.2f}%"
                )
            
            with col4:
                st.metric(
                    "Active Users",
                    value=kpis.get("active_users", 0),
                    delta=calculate_delta("active_users", kpis.get("active_users", 0))
                )
        
        # Show data freshness
        st.info(f"📅 Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        st.error(f"❌ Failed to fetch metrics: {str(e)}")

def calculate_delta(metric_name: str, current_value: float) -> float:
    """Calculate percentage change from previous value"""
    # Store previous values in session state
    if "previous_metrics" not in st.session_state:
        st.session_state.previous_metrics = {}
    
    previous_value = st.session_state.previous_metrics.get(metric_name, current_value)
    st.session_state.previous_metrics[metric_name] = current_value
    
    if previous_value == 0:
        return 0
    
    return ((current_value - previous_value) / previous_value) * 100
```

#### Pipeline Health Monitoring
```python
def show_pipeline_health():
    """Monitor streaming pipeline health and performance"""
    st.subheader("🔄 Pipeline Health")
    
    # Check streaming query status
    try:
        latency_response = requests.get(f"{API_URL}/latency", timeout=5)
        latency_data = latency_response.json()
        
        # Display latency metrics
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Pipeline Latency")
            latency_df = pd.DataFrame(latency_data)
            
            if not latency_df.empty:
                # Create latency trend chart
                fig = px.line(
                    latency_df,
                    x="minute",
                    y="avg_ingestion_lag",
                    title="Average Ingestion Lag (seconds)",
                    color_discrete_sequence=["#1f77b4"]
                )
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)
                
                # Show latency status
                latest_latency = latency_df.iloc[0]["avg_ingestion_lag"]
                latency_status = latency_df.iloc[0]["latency_status"]
                
                status_colors = {
                    "excellent": "🟢",
                    "good": "🟡", 
                    "warning": "🟠",
                    "critical": "🔴"
                }
                
                st.metric(
                    "Current Latency",
                    value=f"{latest_latency:.2f}s",
                    delta=f"{status_colors.get(latency_status, '❓')} {latency_status.title()}"
                )
        
        with col2:
            st.subheader("Processing Throughput")
            
            # Show events processed per minute
            if not latency_df.empty:
                throughput_fig = px.bar(
                    latency_df,
                    x="minute",
                    y="events_processed",
                    title="Events Processed per Minute",
                    color_discrete_sequence=["#2ca02c"]
                )
                throughput_fig.update_layout(height=300)
                st.plotly_chart(throughput_fig, use_container_width=True)
                
                total_events = latency_df["events_processed"].sum()
                avg_throughput = latency_df["events_processed"].mean()
                
                st.metric(
                    "Total Events (10min)",
                    value=f"{total_events:,}",
                    delta=f"Avg: {avg_throughput:.0f}/min"
                )
    
    except Exception as e:
        st.error(f"❌ Failed to fetch pipeline metrics: {str(e)}")
```

#### Data Quality Dashboard
```python
def show_data_quality_metrics():
    """Display comprehensive data quality metrics"""
    st.subheader("🔍 Data Quality Monitoring")
    
    try:
        dq_response = requests.get(f"{API_URL}/dq/status", timeout=5)
        dq_data = dq_response.json()
        
        # Create tabs for different DQ dimensions
        tab1, tab2, tab3, tab4 = st.tabs(["Completeness", "Validity", "Timeliness", "Consistency"])
        
        with tab1:
            st.subheader("Data Completeness")
            completeness = dq_data.get("completeness", {})
            
            col1, col2 = st.columns(2)
            with col1:
                null_rate = (1 - completeness.get("null_rate", 0)) * 100
                st.metric("Non-Null Rate", f"{null_rate:.2f}%")
            with col2:
                empty_rate = (1 - completeness.get("empty_rate", 0)) * 100
                st.metric("Non-Empty Rate", f"{empty_rate:.2f}%")
        
        with tab2:
            st.subheader("Data Validity")
            validity = dq_data.get("validity", {})
            
            col1, col2 = st.columns(2)
            with col1:
                price_valid = validity.get("price_range", 0) * 100
                st.metric("Valid Price Range", f"{price_valid:.2f}%")
            with col2:
                event_valid = validity.get("event_types", 0) * 100
                st.metric("Valid Event Types", f"{event_valid:.2f}%")
        
        with tab3:
            st.subheader("Data Timeliness")
            timeliness = dq_data.get("timeliness", {})
            
            col1, col2 = st.columns(2)
            with col1:
                freshness = timeliness.get("freshness", 0)
                st.metric("Data Freshness", f"{freshness:.1f} min")
            with col2:
                lag = timeliness.get("lag", 0)
                st.metric("Ingestion Lag", f"{lag:.1f} min")
        
        with tab4:
            st.subheader("Data Consistency")
            consistency = dq_data.get("consistency", {})
            
            col1, col2 = st.columns(2)
            with col1:
                duplicate_rate = (1 - consistency.get("duplicate_rate", 0)) * 100
                st.metric("Unique Rate", f"{duplicate_rate:.2f}%")
            with col2:
                schema_compliance = consistency.get("schema_compliance", 0) * 100
                st.metric("Schema Compliance", f"{schema_compliance:.2f}%")
                
    except Exception as e:
        st.error(f"❌ Failed to fetch data quality metrics: {str(e)}")
```

### Service Health Checks
```python
def check_kafka_health() -> bool:
    """Check Kafka broker health"""
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_URL],
            request_timeout_ms=5000
        )
        producer.close()
        return True
    except Exception:
        return False

def check_spark_health() -> bool:
    """Check Spark streaming job health"""
    try:
        # Check if streaming queries are running
        # This would connect to Spark UI or check streaming query status
        return True  # Placeholder implementation
    except Exception:
        return False

def check_api_health() -> bool:
    """Check FastAPI service health"""
    try:
        response = requests.get(f"{API_URL}/health", timeout=5)
        return response.status_code == 200
    except Exception:
        return False

def check_frontend_health() -> bool:
    """Check React frontend availability"""
    try:
        response = requests.get("http://frontend:3000", timeout=5)
        return response.status_code == 200
    except Exception:
        return False
```

## Health Check Endpoints

### FastAPI Health Monitoring
```python
# Enhanced health checks in assistant_api.py

@app.get("/health")
async def health():
    """Basic health check with system metrics"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "uptime": time.time() - _start_time,
        "cache_stats": _cache_stats,
        "memory_usage": get_memory_usage(),
        "active_connections": get_active_connections()
    }

@app.get("/health/detailed")
async def detailed_health():
    """Comprehensive health check with dependency status"""
    health_status = {
        "service": "healthy",
        "timestamp": datetime.now().isoformat(),
        "checks": {}
    }
    
    # Database connectivity
    try:
        duckdb_conn.execute("SELECT 1").fetchone()
        health_status["checks"]["database"] = "healthy"
    except Exception as e:
        health_status["checks"]["database"] = f"unhealthy: {str(e)}"
        health_status["service"] = "degraded"
    
    # Data freshness check
    try:
        result = duckdb_conn.execute(
            "SELECT EXTRACT(epoch FROM NOW() - MAX(ts)) / 60 as minutes_old FROM silver_events"
        ).fetchone()
        
        minutes_old = result[0] if result else float('inf')
        if minutes_old < 5:
            health_status["checks"]["data_freshness"] = "healthy"
        elif minutes_old < 15:
            health_status["checks"]["data_freshness"] = "warning"
            health_status["service"] = "degraded"
        else:
            health_status["checks"]["data_freshness"] = "critical"
            health_status["service"] = "unhealthy"
            
    except Exception as e:
        health_status["checks"]["data_freshness"] = f"error: {str(e)}"
        health_status["service"] = "degraded"
    
    # Cache performance
    cache_hit_rate = _cache_stats["hits"] / max(_cache_stats["hits"] + _cache_stats["misses"], 1)
    health_status["checks"]["cache_performance"] = {
        "hit_rate": cache_hit_rate,
        "status": "healthy" if cache_hit_rate > 0.7 else "warning"
    }
    
    return health_status
```

### Kubernetes Readiness/Liveness Probes
```python
@app.get("/healthz")
async def kubernetes_health():
    """Kubernetes-style health check"""
    try:
        # Quick database check
        duckdb_conn.execute("SELECT 1").fetchone()
        
        # Check data recency (must be less than 10 minutes old)
        result = duckdb_conn.execute(
            "SELECT EXTRACT(epoch FROM NOW() - MAX(ts)) / 60 FROM silver_events"
        ).fetchone()
        
        if result and result[0] > 10:
            raise HTTPException(status_code=503, detail="Data too stale")
        
        return {"status": "ok"}
        
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Health check failed: {str(e)}")

@app.get("/readyz")
async def kubernetes_readiness():
    """Kubernetes readiness probe"""
    try:
        # Ensure all dependencies are ready
        duckdb_conn.execute("SELECT COUNT(*) FROM silver_events").fetchone()
        
        # Check if we have recent data
        result = duckdb_conn.execute(
            "SELECT COUNT(*) FROM gold_kpis WHERE window_start >= NOW() - INTERVAL 5 MINUTES"
        ).fetchone()
        
        if not result or result[0] == 0:
            raise HTTPException(status_code=503, detail="No recent aggregated data")
        
        return {"status": "ready"}
        
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Readiness check failed: {str(e)}")
```

## Alerting & Notifications

### Alert Configuration
```python
# Alert thresholds and configuration
ALERT_CONFIG = {
    "conversion_rate": {
        "threshold": 0.05,  # 5% minimum conversion rate
        "window": "5 minutes",
        "severity": "warning"
    },
    "latency": {
        "threshold": 30,  # 30 seconds max latency
        "window": "1 minute", 
        "severity": "critical"
    },
    "data_freshness": {
        "threshold": 300,  # 5 minutes max data age
        "window": "continuous",
        "severity": "critical"
    },
    "error_rate": {
        "threshold": 0.05,  # 5% max error rate
        "window": "5 minutes",
        "severity": "warning"
    }
}

@app.get("/alerts/current")
async def get_current_alerts():
    """Get all currently active alerts"""
    alerts = []
    
    # Check conversion rate
    conversion_alert = check_conversion_rate_alert()
    if conversion_alert:
        alerts.append(conversion_alert)
    
    # Check latency
    latency_alert = check_latency_alert()
    if latency_alert:
        alerts.append(latency_alert)
    
    # Check data freshness
    freshness_alert = check_data_freshness_alert()
    if freshness_alert:
        alerts.append(freshness_alert)
    
    return {
        "active_alerts": len(alerts),
        "alerts": alerts,
        "timestamp": datetime.now().isoformat()
    }

def check_conversion_rate_alert():
    """Check if conversion rate is below threshold"""
    try:
        result = duckdb_conn.execute("""
            SELECT AVG(conversion_rate) as avg_conversion
            FROM gold_kpis 
            WHERE window_start >= NOW() - INTERVAL 5 MINUTES
        """).fetchone()
        
        if result and result[0] < ALERT_CONFIG["conversion_rate"]["threshold"]:
            return {
                "type": "conversion_rate",
                "severity": "warning",
                "message": f"Conversion rate {result[0]:.3f} below threshold {ALERT_CONFIG['conversion_rate']['threshold']}",
                "timestamp": datetime.now().isoformat(),
                "value": result[0],
                "threshold": ALERT_CONFIG["conversion_rate"]["threshold"]
            }
    except Exception as e:
        logger.error(f"Error checking conversion rate alert: {e}")
    
    return None
```

## Performance Monitoring

### System Metrics Collection
```python
import psutil
import resource

def get_system_metrics():
    """Collect system performance metrics"""
    return {
        "cpu": {
            "usage_percent": psutil.cpu_percent(interval=1),
            "count": psutil.cpu_count(),
            "load_avg": os.getloadavg() if hasattr(os, 'getloadavg') else None
        },
        "memory": {
            "total": psutil.virtual_memory().total,
            "available": psutil.virtual_memory().available,
            "used_percent": psutil.virtual_memory().percent,
            "buffers": psutil.virtual_memory().buffers,
            "cached": psutil.virtual_memory().cached
        },
        "disk": {
            "usage_percent": psutil.disk_usage("/").percent,
            "free_bytes": psutil.disk_usage("/").free,
            "total_bytes": psutil.disk_usage("/").total
        },
        "network": {
            "bytes_sent": psutil.net_io_counters().bytes_sent,
            "bytes_recv": psutil.net_io_counters().bytes_recv,
            "packets_sent": psutil.net_io_counters().packets_sent,
            "packets_recv": psutil.net_io_counters().packets_recv
        }
    }

@app.get("/metrics/system")
async def system_metrics():
    """Get current system performance metrics"""
    return get_system_metrics()
```

### Database Performance
```python
@app.get("/metrics/database")
async def database_metrics():
    """Get database performance metrics"""
    try:
        # Query execution metrics
        query_stats = duckdb_conn.execute("""
            SELECT 
                COUNT(*) as total_queries,
                AVG(query_time) as avg_query_time,
                MAX(query_time) as max_query_time
            FROM query_log 
            WHERE timestamp >= NOW() - INTERVAL 1 HOUR
        """).fetchone()
        
        # Table statistics
        table_stats = duckdb_conn.execute("""
            SELECT 
                'silver_events' as table_name,
                COUNT(*) as row_count,
                pg_size_pretty(pg_total_relation_size('silver_events')) as size
            FROM silver_events
            UNION ALL
            SELECT 
                'gold_kpis',
                COUNT(*),
                pg_size_pretty(pg_total_relation_size('gold_kpis'))
            FROM gold_kpis
        """).fetchall()
        
        return {
            "query_performance": {
                "total_queries": query_stats[0] if query_stats else 0,
                "avg_query_time_ms": query_stats[1] if query_stats else 0,
                "max_query_time_ms": query_stats[2] if query_stats else 0
            },
            "table_statistics": [
                {
                    "table": row[0],
                    "rows": row[1], 
                    "size": row[2]
                } for row in table_stats
            ]
        }
        
    except Exception as e:
        logger.error(f"Error collecting database metrics: {e}")
        return {"error": str(e)}
```

## Operational Runbooks

### Common Issues & Solutions

#### High Latency
```markdown
## High Pipeline Latency (>30s)

### Symptoms
- Dashboard shows latency_status: "warning" or "critical"
- Events taking >30 seconds from ingestion to gold layer

### Investigation Steps
1. Check Spark streaming UI: http://localhost:4040
2. Verify Kafka lag: `docker exec rt_kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group spark-streaming`
3. Check memory usage: API endpoint `/metrics/system`

### Resolution
1. Restart Spark streaming: `docker restart rt_spark`
2. Clear checkpoints if corrupted: `rm -rf data/checkpoints/*`
3. Increase memory if needed: Update docker-compose.yml
```

#### Data Quality Issues
```markdown
## Data Quality Degradation

### Symptoms
- High null/empty rates in completeness checks
- Schema compliance below 100%
- Duplicate rate increasing

### Investigation Steps
1. Check producer logs: `docker logs rt_producer`
2. Verify event schema: API endpoint `/dq/status`
3. Check Silver layer filtering: Review pipeline logs

### Resolution
1. Fix producer data generation
2. Update schema validation rules
3. Increase deduplication window if needed
```

### Maintenance Procedures

#### Daily Maintenance
```bash
#!/bin/bash
# Daily maintenance script

echo "Starting daily maintenance..."

# Optimize Delta tables
echo "Optimizing Delta tables..."
docker exec rt_spark python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.sql('OPTIMIZE delta.`/delta/bronze_events`')
spark.sql('OPTIMIZE delta.`/delta/silver_events`') 
spark.sql('OPTIMIZE delta.`/delta/gold_kpis`')
"

# Clean old logs
echo "Cleaning old logs..."
find logs/ -name "*.log" -mtime +7 -delete

# Check disk usage
echo "Checking disk usage..."
df -h

echo "Daily maintenance complete."
```

#### Weekly Maintenance
```bash
#!/bin/bash
# Weekly maintenance script

echo "Starting weekly maintenance..."

# Vacuum old Delta versions (keep 7 days)
echo "Vacuuming Delta tables..."
docker exec rt_spark python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.sql('VACUUM delta.`/delta/bronze_events` RETAIN 168 HOURS')
spark.sql('VACUUM delta.`/delta/silver_events` RETAIN 168 HOURS')
spark.sql('VACUUM delta.`/delta/gold_kpis` RETAIN 168 HOURS')
"

# Backup configurations
echo "Backing up configurations..."
tar -czf "backups/config-$(date +%Y%m%d).tar.gz" docker-compose.yml pipelines/ services/

echo "Weekly maintenance complete."
```
