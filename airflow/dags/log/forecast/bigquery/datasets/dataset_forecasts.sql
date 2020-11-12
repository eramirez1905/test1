CREATE TEMPORARY FUNCTION get_created_date(X ANY TYPE) AS
(
  DATE_TRUNC(CAST(X AS DATE), ISOWEEK)
); 

CREATE OR REPLACE TABLE forecasting.dataset_forecasts
PARTITION BY created_date
CLUSTER BY country_code AS
WITH forecasts AS
(
  SELECT
    country_code,
    zone_id,
    datetime,
    model_name, 
    created_at,
    forecast_date,
    d,
    orders,
    is_most_recent,
    is_valid
  FROM `forecasting.orders_forecasts_timeseries` f
  WHERE 
    -- 2019-07-10 is when we started logging forecast outputs
    created_date >= '2019-07-10'
),
models AS
(
  SELECT 
    DISTINCT model_name
  FROM forecasts
  WHERE
    is_valid
)
SELECT 
  t.country_code,
  t.zone_id,
  t.city_id,
  t.datetime,
  t.datetime_local,
  COALESCE(o.orders, 0) + COALESCE(ol.orders_lost_net, 0) AS orders_hist, 
  m.model_name,
  f.created_at,
  f.forecast_date,
  f.d,
  f.orders AS orders_forecasted,
  f.orders - (COALESCE(o.orders, 0) + COALESCE(ol.orders_lost_net, 0)) AS residual,
  f.is_valid,
  f.is_most_recent,
  f.orders IS NOT NULL AS has_forecast,
  get_created_date(f.created_at) AS created_date
FROM `forecasting.timeseries` t
LEFT JOIN `forecasting.orders_timeseries` o
  ON o.country_code = t.country_code
  AND o.zone_id = t.zone_id
  AND o.datetime = t.datetime
LEFT JOIN `forecasting.orders_lost_timeseries` ol
  ON ol.country_code = t.country_code
  AND ol.zone_id = t.zone_id
  AND ol.datetime = t.datetime
-- cross join to detect missing models
CROSS JOIN models m
LEFT JOIN forecasts f 
  ON f.country_code = t.country_code
  AND f.zone_id = t.zone_id
  AND f.datetime = t.datetime
  AND f.model_name = m.model_name
WHERE 
  -- 2019-07-10 is when we started ds_model_outputs.order_forecasts
  DATE(t.datetime_local) >= '2019-07-10'
  AND t.datetime <= TIMESTAMP('{{next_execution_date}}')
