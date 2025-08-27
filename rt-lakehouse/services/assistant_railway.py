# Simplified FastAPI Assistant for Railway Deployment
import os
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import pandas as pd
import json
import asyncio
from contextlib import asynccontextmanager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
DEBUG = os.getenv("DEBUG", "false").lower() == "true"
PORT = int(os.getenv("PORT", 8000))

# Mock data for Railway demo (since we don't have full infrastructure)
MOCK_EVENTS = [
    {
        "id": f"event_{i}",
        "timestamp": datetime.now().isoformat(),
        "event_type": "user_action",
        "user_id": f"user_{i % 100}",
        "session_id": f"session_{i % 50}",
        "data": {"action": "click", "page": f"page_{i % 10}"}
    }
    for i in range(1000)
]

MOCK_METRICS = {
    "total_events": len(MOCK_EVENTS),
    "active_users": 100,
    "conversion_rate": 0.15,
    "avg_session_duration": 245.6,
    "top_pages": [
        {"page": "homepage", "views": 450},
        {"page": "products", "views": 320},
        {"page": "checkout", "views": 180},
    ]
}

# Pydantic models
class EventModel(BaseModel):
    event_type: str
    user_id: str
    session_id: str
    data: Dict[str, Any]

class QueryRequest(BaseModel):
    query: str
    filters: Optional[Dict[str, Any]] = None

class MetricsResponse(BaseModel):
    metrics: Dict[str, Any]
    timestamp: str

# Application lifespan
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting RT-Lakehouse Assistant API (Railway Demo Mode)")
    yield
    logger.info("Shutting down RT-Lakehouse Assistant API")

# Create FastAPI app
app = FastAPI(
    title="RT-Lakehouse Assistant API",
    description="Real-time Data Lakehouse Assistant with Analytics and Time Travel",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Configure CORS
cors_origins = os.getenv("CORS_ORIGINS", '["*"]')
try:
    origins = json.loads(cors_origins)
except Exception:
    origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint for monitoring"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "environment": ENVIRONMENT,
        "service": "rt-lakehouse-assistant"
    }

# Root endpoint
@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "RT-Lakehouse Assistant API",
        "version": "1.0.0",
        "environment": ENVIRONMENT,
        "docs": "/docs",
        "health": "/health",
        "demo_mode": True
    }

# Events endpoints
@app.get("/api/events")
async def get_events(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    event_type: Optional[str] = None
):
    """Get events with pagination and filtering"""
    try:
        events = MOCK_EVENTS.copy()
        
        # Apply filters
        if event_type:
            events = [e for e in events if e.get("event_type") == event_type]
        
        # Apply pagination
        total = len(events)
        paginated_events = events[offset:offset + limit]
        
        return {
            "events": paginated_events,
            "total": total,
            "limit": limit,
            "offset": offset,
            "has_more": offset + limit < total
        }
    except Exception as e:
        logger.error(f"Error fetching events: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch events")

@app.post("/api/events")
async def create_event(event: EventModel):
    """Create a new event"""
    try:
        new_event = {
            "id": f"event_{len(MOCK_EVENTS) + 1}",
            "timestamp": datetime.now().isoformat(),
            **event.dict()
        }
        MOCK_EVENTS.append(new_event)
        
        return {
            "message": "Event created successfully",
            "event": new_event
        }
    except Exception as e:
        logger.error(f"Error creating event: {e}")
        raise HTTPException(status_code=500, detail="Failed to create event")

# Analytics endpoints
@app.get("/api/analytics/metrics")
async def get_metrics():
    """Get real-time analytics metrics"""
    try:
        return MetricsResponse(
            metrics=MOCK_METRICS,
            timestamp=datetime.now().isoformat()
        )
    except Exception as e:
        logger.error(f"Error fetching metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch metrics")

@app.get("/api/analytics/events/count")
async def get_event_count(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    event_type: Optional[str] = None
):
    """Get event count with optional filtering"""
    try:
        # Simulate event counting
        count = len(MOCK_EVENTS)
        if event_type:
            count = len([e for e in MOCK_EVENTS if e.get("event_type") == event_type])
        
        return {
            "count": count,
            "filters": {
                "start_date": start_date,
                "end_date": end_date,
                "event_type": event_type
            },
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error counting events: {e}")
        raise HTTPException(status_code=500, detail="Failed to count events")

# Query endpoints
@app.post("/api/query/sql")
async def execute_sql_query(request: QueryRequest):
    """Execute SQL query (demo mode - returns mock data)"""
    try:
        # Simulate SQL query execution
        mock_results = [
            {"user_id": f"user_{i}", "total_events": i * 5, "last_seen": datetime.now().isoformat()}
            for i in range(1, 11)
        ]
        
        return {
            "query": request.query,
            "results": mock_results,
            "row_count": len(mock_results),
            "execution_time_ms": 45.2,
            "demo_mode": True
        }
    except Exception as e:
        logger.error(f"Error executing SQL query: {e}")
        raise HTTPException(status_code=500, detail="Failed to execute query")

@app.post("/api/query/natural")
async def natural_language_query(request: QueryRequest):
    """Natural language query processing (demo mode)"""
    try:
        # Simulate natural language processing
        mock_response = {
            "query": request.query,
            "interpreted_sql": "SELECT user_id, COUNT(*) as event_count FROM events GROUP BY user_id LIMIT 10",
            "results": [
                {"user_id": f"user_{i}", "event_count": i * 7}
                for i in range(1, 11)
            ],
            "confidence": 0.87,
            "demo_mode": True
        }
        
        return mock_response
    except Exception as e:
        logger.error(f"Error processing natural language query: {e}")
        raise HTTPException(status_code=500, detail="Failed to process query")

# Demo endpoints
@app.get("/api/demo/status")
async def demo_status():
    """Demo status and available features"""
    return {
        "demo_mode": True,
        "available_features": [
            "Event ingestion and retrieval",
            "Real-time analytics metrics",
            "SQL query simulation",
            "Natural language query processing",
            "Event counting and filtering"
        ],
        "limitations": [
            "Uses mock data instead of real Delta Lake",
            "No actual Kafka streaming",
            "Simplified ML features",
            "No persistent storage"
        ],
        "endpoints": {
            "health": "/health",
            "events": "/api/events",
            "metrics": "/api/analytics/metrics",
            "sql_query": "/api/query/sql",
            "natural_query": "/api/query/natural"
        }
    }

# Error handlers
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    logger.error(f"Global exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error", "demo_mode": True}
    )

if __name__ == "__main__":
    import uvicorn
    
    logger.info(f"Starting RT-Lakehouse Assistant API on port {PORT}")
    logger.info(f"Environment: {ENVIRONMENT}")
    logger.info(f"Demo mode: True")
    
    uvicorn.run(
        "assistant_railway:app",
        host="0.0.0.0",
        port=PORT,
        reload=DEBUG,
        log_level="info"
    )
