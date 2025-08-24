# Real-Time Lakehouse Analytics (Kafka -> Delta -> Dashboard)

This section of the repo delivers a working streaming pipeline (Kafka -> Bronze -> Silver -> Gold on Delta Lake) powering live KPIs and a Databricks SQL dashboard, plus batch backfills and optional anomaly alerts.

## Quick Start

1. Create UC objects
```sql
CREATE CATALOG IF NOT EXISTS demo;
CREATE SCHEMA  IF NOT EXISTS demo.ecommerce_rt;
```

2. Run notebooks in order (attach a cluster):
- pipelines/00_config.py (or `%run ./00_config` in each)
- pipelines/10_bronze_from_kafka.py
- pipelines/20_silver_transform.py
- pipelines/30_gold_kpis.py

3. Start producer (locally)
```bash
pip install kafka-python
export KAFKA_BOOTSTRAP=localhost:9092
export KAFKA_TOPIC=ecommerce_events
python producers/kafka_event_producer.py
```

4. Create SQL views from `sql/dashboard_views.sql` and build a dashboard.

5. Optional
- Run `pipelines/90_backfill_batch.py` to load history.
- Run `pipelines/40_alerts_stream.py` to enable anomaly alerts.
- Housekeeping: `sql/optimize_maintenance.sql`.

## Event Contract
See `pipelines/00_config.py` for `event_schema`. Example event:
```json
{
  "event_id": "uuid",
  "user_id": "u-12345",
  "product_id": "p-873",
  "event_type": "page_view | add_to_cart | purchase",
  "price": 19.99,
  "quantity": 1,
  "currency": "USD",
  "ts": "2025-08-24T17:13:25.123Z",
  "ua": "Mozilla/5.0",
  "country": "US"
}
```