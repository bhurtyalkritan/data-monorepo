-- Create knowledge base table for assistant
CREATE TABLE IF NOT EXISTS demo.ecommerce_rt.kb_docs (
  id STRING,
  kind STRING,             -- 'table','column','kpi','example','rule'
  title STRING,
  body STRING,
  table_name STRING,
  column_name STRING,
  updated_at TIMESTAMP
) USING DELTA;

-- Minimal seeds
INSERT INTO demo.ecommerce_rt.kb_docs VALUES
  ('t1','table','gold_kpis',
   'Minute-level KPIs from streaming: orders, gmv, active_users, conversion_rate. Keys: window_start, window_end.',
   'gold_kpis', NULL, current_timestamp()),
  ('k1','kpi','conversion_rate',
   'purchase_users / NULLIF(view_users,0). Use 1-minute tumbling windows; aggregate over time ranges.',
   'gold_kpis', 'conversion_rate', current_timestamp()),
  ('r1','rule','guardrails',
   'Generate ANSI SQL that runs on Databricks. Only SELECTs. Always include a LIMIT when returning raw rows.',
   NULL,NULL,current_timestamp()),
  ('e1','example','question->sql',
   'Q: What was GMV by minute in the last 15 minutes?\nSQL: SELECT window_start, gmv FROM demo.ecommerce_rt.gold_kpis WHERE window_start >= now() - INTERVAL 15 minutes ORDER BY window_start;',
   NULL,NULL,current_timestamp());
