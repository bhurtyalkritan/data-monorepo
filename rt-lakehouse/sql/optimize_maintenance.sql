OPTIMIZE demo.ecommerce_rt.silver_events ZORDER BY (ts, product_id);
OPTIMIZE demo.ecommerce_rt.gold_kpis    ZORDER BY (window_start);

VACUUM demo.ecommerce_rt.silver_events RETAIN 336 HOURS;
VACUUM demo.ecommerce_rt.gold_kpis    RETAIN 336 HOURS;
