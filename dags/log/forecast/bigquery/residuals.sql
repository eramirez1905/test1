CREATE TEMPORARY FUNCTION get_created_date(X ANY TYPE) AS
(
  DATE_TRUNC(CAST(X AS DATE), ISOWEEK)
);

-- Delete yesterday's forecasts and true values to avoid duplicates
DELETE FROM forecasting.residuals
WHERE DATE(datetime_local) = '{{ ds }}';

-- Insert yesterday's forecasts and true values
INSERT INTO forecasting.residuals
WITH forecasts_raw AS
(
  SELECT
    f.country_code,
    f.zone_id,
    f.model_name,
    ff.created_at,
    ff.forecast_for AS datetime,
    ff.forecast_for_local AS datetime_local,
    f.created_date AS forecast_date,
    DATE_DIFF(DATE(ff.forecast_for_local), f.created_date, DAY) AS d,
    ff.orders,
    ff.is_most_recent,
    ff.is_valid
  -- there is no point looking at staging residuals as we run experimental models on production 
  FROM `fulfillment-dwh-production.cl.order_forecasts` f,
  UNNEST(forecasts) ff
  WHERE
    -- 2019-07-10 is when we started logging forecast outputs
    created_date >= GREATEST(DATE_SUB('{{ next_ds }}', INTERVAL 6 WEEK), '2019-07-10')
),
forecasts AS
(
SELECT
  *,
  ROW_NUMBER() OVER (PARTITION BY country_code, zone_id, model_name, datetime, d ORDER BY created_at DESC) AS _row_number
FROM forecasts_raw
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
  d.country_code,
  d.zone_id,
  d.city_id,
  d.datetime,
  DATETIME(d.datetime, d.timezone) AS datetime_local,
  COALESCE(d.orders, 0) + COALESCE(d.orders_lost_net, 0) AS orders_hist,
  m.model_name,
  f.created_at,
  f.forecast_date,
  f.d,
  f.orders AS orders_forecasted,
  f.orders - (COALESCE(d.orders, 0) + COALESCE(d.orders_lost_net, 0)) AS residual,
  ABS(f.orders - (COALESCE(d.orders, 0) + COALESCE(d.orders_lost_net, 0))) AS residual_abs,
  POW(f.orders - (COALESCE(d.orders, 0) + COALESCE(d.orders_lost_net, 0)), 2) AS residual_sqr,
  f.is_valid,
  f.is_most_recent,
  f.orders IS NOT NULL AS has_forecast,
  get_created_date(f.created_at) AS created_date
FROM `forecasting.dataset_zone_hour` d
-- cross join to detect missing models
CROSS JOIN models m
LEFT JOIN forecasts f
  ON f.country_code = d.country_code
  AND f.zone_id = d.zone_id
  AND f.datetime = d.datetime
  AND f.model_name = m.model_name
  AND f._row_number = 1
WHERE
  DATE(DATETIME(d.datetime, d.timezone)) = '{{ ds }}'
  AND f.created_at IS NOT NULL
  AND f.d >= 1
