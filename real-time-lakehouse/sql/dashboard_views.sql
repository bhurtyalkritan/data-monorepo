CREATE OR REPLACE VIEW demo.ecommerce_rt.v_kpi_timeseries AS
SELECT window_start, window_end, orders, gmv, active_users, conversion_rate
FROM demo.ecommerce_rt.gold_kpis
ORDER BY window_start DESC;

CREATE OR REPLACE VIEW demo.ecommerce_rt.v_kpi_last_15m AS
SELECT
  SUM(orders) AS orders_15m,
  SUM(gmv) AS gmv_15m,
  AVG(conversion_rate) AS conversion_15m,
  MAX(window_start) AS last_window
FROM demo.ecommerce_rt.gold_kpis
WHERE window_start >= now() - INTERVAL 15 minutes;
