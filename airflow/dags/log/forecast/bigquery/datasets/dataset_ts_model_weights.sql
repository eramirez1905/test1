CREATE TEMP FUNCTION extract_metadata(json STRING)
RETURNS ARRAY<STRING>
LANGUAGE js AS """
  if (JSON.parse(json) == null) {
      return [];
  } else {
      return JSON.parse(json).map(x=>[x.model_name, x.model_type, x.weight, x.value]);
  }
""";

CREATE OR REPLACE TABLE forecasting.dataset_ts_model_weights AS
WITH
-- get predictions from all individual models by extracting from farnsworth regression
model_outputs AS (
  SELECT
    country_code,
    zone_id,
    datetime,
    forecast_date,
    model_name,
    orders,
    created_at,
    extract_metadata(metadata) AS metadata
  FROM `fulfillment-dwh-production.ds_model_outputs.order_forecasts`
  WHERE
    forecast_date >= DATE_SUB('{{ next_ds  }}', INTERVAL 42 DAY)
    AND model_name = 'farnsworth_regression'
),
-- tidy
individual_model_outputs AS
(
  SELECT
    country_code,
    zone_id,
    datetime,
    forecast_date,
    SPLIT(metadata_unnested, ',')[OFFSET(0)] AS model_name,
    SPLIT(metadata_unnested, ',')[OFFSET(1)] AS model_type,
    CAST(SPLIT(metadata_unnested, ',')[OFFSET(3)] AS FLOAT64) AS value
  FROM model_outputs, UNNEST(metadata) AS metadata_unnested
),
-- compute residuals
residuals AS
(
  SELECT
    f.country_code,
    f.zone_id,
    f.datetime,
    f.model_name,
    f.model_type,
    DATE_DIFF(DATE(f.datetime), f.forecast_date, DAY) AS d,
    f.value AS orders_forecasted,
    o.orders AS orders,
    f.value - o.orders AS residual,
  FROM individual_model_outputs f
  LEFT JOIN forecasting.orders_timeseries o USING (country_code, zone_id, datetime)
  WHERE
    o.orders IS NOT NULL
    -- needed temporarily to exclude model
    AND f.model_name <> 'nnet'
),
errors AS
(
  SELECT
    country_code,
    model_name,
    model_type,
    AVG(POW(residual, 4)) AS error_metric,
    COUNT(*) AS n
  FROM residuals
  WHERE
    d BETWEEN 2 AND 14
  GROUP BY 1, 2, 3
),
error_cutoff AS
(
-- workaround, because percentile_cont doesn't return a single value per country_code
  SELECT
    country_code,
    AVG(cutoff) AS cutoff
  FROM (
    SELECT
      country_code,
      PERCENTILE_CONT(error_metric, 0.75) OVER(PARTITION BY country_code) AS cutoff
    FROM errors
  )
  GROUP BY country_code
),
weights AS
(
  SELECT
    country_code,
    model_name,
    model_type,
    cutoff,
    ROUND((1 / error_metric) / SUM((1 / error_metric)) OVER(PARTITION BY country_code), 5) AS weight,
  FROM errors
  LEFT JOIN error_cutoff
  USING (country_code)
  WHERE error_metric <= cutoff
)
SELECT
  country_code,
  model_name,
  model_type,
  weight,
  DATE('{{ next_ds }}') AS updated_date
FROM weights
