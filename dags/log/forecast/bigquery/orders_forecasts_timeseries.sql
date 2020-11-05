CREATE OR REPLACE TABLE forecasting.orders_forecasts_timeseries
PARTITION BY created_date
CLUSTER BY country_code AS
WITH 
forecasts_ds_raw AS
(
  SELECT
    f.country_code,
    f.zone_id,
    f.datetime,
    f.timezone,
    f.model_name, 
    f.created_at,
    f.forecast_date,
    f.orders
  FROM `ds_model_outputs.order_forecasts` f
  LEFT JOIN `forecasting.zones` z
    ON z.country_code = f.country_code
    AND z.zone_id = f.zone_id
  WHERE 
    -- only consider zones that still exist
    z.zone_id IS NOT NULL
),
forecasts_rooster_raw AS
(
  SELECT
    f.country_code,
    f.zone_id,
    f.forecast_for AS datetime,
    z.timezone,
    'rooster' AS model_name,
    f.created_at,
    DATE(DATETIME(f.created_at, z.timezone)) AS forecast_date,
    f.orders
  FROM `fulfillment-dwh-production.dl.forecast_orders_forecasts` f
  LEFT JOIN `forecasting.zones` z
    ON z.country_code = f.country_code
    AND z.zone_id = f.zone_id
  WHERE 
    -- only consider zones that still exist
    z.zone_id IS NOT NULL
),
forecasts_rooster_adjusted_raw AS
(
  SELECT
    f.country_code,
    f.zone_id,
    f.forecast_for AS datetime,
    z.timezone,
    'rooster_adjusted' AS model_name,
    f.created_at,
    DATE(DATETIME(f.created_at, z.timezone)) AS forecast_date,
    f.adjusted_orders AS orders
  FROM `fulfillment-dwh-production.dl.forecast_adjusted_forecasts` f
  LEFT JOIN `forecasting.zones` z
    ON z.country_code = f.country_code
    AND z.zone_id = f.zone_id
  WHERE
    -- only consider zones that still exist
    z.zone_id IS NOT NULL
),
forecasts_all_raw AS
(
  SELECT * FROM forecasts_ds_raw
  UNION ALL
  SELECT * FROM forecasts_rooster_raw
  UNION ALL
  SELECT * FROM forecasts_rooster_adjusted_raw
),
forecasts_ranked AS
(
  SELECT
    country_code,
    zone_id,
    datetime,
    timezone,
    model_name, 
    created_at,
    orders,
    forecast_date,
    DATE_DIFF(DATE(DATETIME(datetime, timezone)), forecast_date, DAY) AS d,
    ROW_NUMBER() OVER (PARTITION BY country_code, zone_id, datetime, model_name, forecast_date ORDER BY created_at DESC) AS rank
  FROM forecasts_all_raw
),
forecasts AS
(
  SELECT 
    * 
  FROM forecasts_ranked
  WHERE
    rank = 1
)
SELECT
  f.* EXCEPT (rank),
  RANK() OVER (PARTITION BY f.country_code, f.zone_id, f.datetime, f.model_name ORDER BY d ASC) AS rank,
  RANK() OVER (PARTITION BY f.country_code, f.zone_id, f.datetime, f.model_name ORDER BY d ASC) = 1 AS is_most_recent,
  -- forecast is only valid if it was produced a few hours after zone was changed for the last time
  TIMESTAMP_DIFF(f.created_at, z.geom_updated_at, DAY) >= 1 AS is_valid,
  f.forecast_date AS created_date
FROM forecasts f
LEFT JOIN `forecasting.zones` z
  ON z.country_code = f.country_code
  AND z.zone_id = f.zone_id
WHERE 
  -- ignore forecasts created after dag run
  f.created_at <= TIMESTAMP('{{next_execution_date}}')
