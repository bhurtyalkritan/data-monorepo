# RT-Lakehouse Architecture Overview

## System Architecture

The RT-Lakehouse implements a modern lakehouse architecture with real-time streaming capabilities, following the medallion pattern (Bronze → Silver → Gold) with Delta Lake as the storage foundation.

```
┌─────────────────┐    ┌─────────────┐    ┌──────────────────┐
│   Event Source  │───▶│    Kafka    │───▶│  Spark Streaming │
│   (Producer)    │    │             │    │    (Bronze)      │
└─────────────────┘    └─────────────┘    └──────────────────┘
                                                    │
                                                    ▼
┌─────────────────┐    ┌─────────────┐    ┌──────────────────┐
│  React Frontend │◀───│  FastAPI    │◀───│     DuckDB       │
│   (Dashboard)   │    │ (Assistant) │    │   (Analytics)    │
└─────────────────┘    └─────────────┘    └──────────────────┘
                                                    ▲
┌─────────────────┐    ┌─────────────┐    ┌──────────────────┘
│   Monitoring    │    │   Qdrant    │    │    Delta Lake    │
│  (Streamlit)    │    │ (Vector DB) │    │ Silver → Gold    │
└─────────────────┘    └─────────────┘    └──────────────────┘
```

## Core Components

### 1. Data Ingestion Layer
- **Kafka Event Producer**: Generates synthetic e-commerce events
- **Apache Kafka**: Message broker for streaming data
- **Zookeeper**: Coordination service for Kafka

### 2. Processing Layer
- **Apache Spark**: Structured streaming engine
- **Delta Lake**: ACID transactions and time travel
- **Medallion Architecture**: Bronze → Silver → Gold data refinement

### 3. Storage Layer
- **Delta Tables**: Versioned, ACID-compliant data storage
- **Parquet Snapshots**: Optimized query performance
- **Time Travel**: Historical point-in-time queries

### 4. Serving Layer
- **FastAPI Assistant**: 60+ endpoints with advanced features
- **DuckDB**: Fast analytical queries over Parquet
- **Caching & Rate Limiting**: Production-ready API features

### 5. Visualization Layer
- **React Dashboard**: Real-time KPI visualization
- **Streamlit Monitoring**: System health and metrics
- **Material-UI v7**: Modern, responsive UI components

### 6. Advanced Features
- **Vector Search**: Qdrant for semantic similarity
- **ML Analytics**: Anomaly detection and forecasting
- **Time Travel**: Delta Lake versioning and history
- **Data Quality**: Automated validation and monitoring

## Data Flow

1. **Event Generation**: Producer generates 100-300 events/sec
2. **Stream Ingestion**: Kafka receives and buffers events
3. **Bronze Processing**: Raw JSON landed in Delta Lake
4. **Silver Transformation**: Data cleaning, deduplication, validation
5. **Gold Aggregation**: Business KPIs and metrics (1-minute windows)
6. **Snapshot Export**: Parquet files for fast analytics
7. **API Serving**: FastAPI exposes real-time endpoints
8. **Dashboard Updates**: React frontend polls and visualizes data

## Technology Stack

### Core Technologies
- **Apache Spark 3.5.1** with Delta Lake 3.2.0
- **Apache Kafka** with Confluent Platform
- **DuckDB** for analytical workloads
- **FastAPI** with async/await patterns
- **React 18** with TypeScript

### Storage & Compute
- **Delta Lake**: ACID transactions, time travel, schema evolution
- **Parquet**: Columnar storage for analytics
- **Docker Compose**: Container orchestration
- **Memory Optimization**: 4GB driver/executor, 6GB containers

### Advanced Capabilities
- **Qdrant Vector Database**: Semantic search and similarity
- **Streamlit**: Interactive monitoring dashboards
- **Material-UI v7**: Modern React components
- **Recharts**: Data visualization library

## Scalability & Performance

### Memory Configuration
- Spark Driver: 4GB heap memory
- Spark Executor: 4GB heap memory
- Container Limits: 6GB per service
- Optimized for development and demo workloads

### Streaming Performance
- Processing Latency: Sub-second
- Throughput: 300+ events/second
- Watermark Handling: 15-second late data tolerance
- Checkpoint Interval: 10 seconds

### Query Performance
- DuckDB Analytics: Sub-100ms response times
- API Caching: ETags and TTL-based invalidation
- Rate Limiting: 60 requests/minute per IP
- Real-time Updates: 1-minute aggregation windows

## Production Considerations

### Monitoring
- Health checks for all services
- Spark streaming metrics
- Pipeline latency tracking
- Data quality monitoring

### Data Governance
- Schema validation and evolution
- Audit logging through Delta Lake
- Time travel for compliance and debugging
- Multi-tenant support capabilities

### Operational Features
- Automated checkpoint management
- OPTIMIZE and VACUUM maintenance
- Backfill capabilities for historical data
- Anomaly detection and alerting
