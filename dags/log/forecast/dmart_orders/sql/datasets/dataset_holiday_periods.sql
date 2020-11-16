CREATE TEMPORARY FUNCTION get_created_date(X ANY TYPE) AS
(
  DATE_TRUNC(CAST(X AS DATE), ISOWEEK)
); 

CREATE OR REPLACE TABLE dmart_order_forecast.dataset_holiday_periods
PARTITION BY created_date
CLUSTER BY country_code AS
WITH 
holiday_periods AS (
  SELECT 
    country_code,
    created_date,
    city_id,
    DATE(start_at_local) AS start_date,
    DATE(end_at_local) AS end_date,
    type,
    -- this is a bit hacky, but depends on the way special days are implemented in rooster
    JSON_EXTRACT_SCALAR(comment, "$.name") AS holiday_period_name,
    comment AS metadata
  FROM dl.rooster_special_day
  WHERE 
    type = 'holiday_period'
    AND active
  ORDER BY country_code, city_id, start_at_local, end_at_local
),
-- used for potential deduplication of user input data of rooster
holiday_periods_unrolled AS
(
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY country_code, city_id, date) AS _row_number
  FROM 
  holiday_periods hp,
  UNNEST(GENERATE_DATE_ARRAY(start_date, end_date, INTERVAL 1 DAY)) AS date
)
SELECT
  country_code,
  city_id,
  date,
  holiday_period_name,
  metadata,
  get_created_date(date) AS created_date
FROM holiday_periods_unrolled
WHERE 
  _row_number = 1