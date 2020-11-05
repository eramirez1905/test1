WITH
dataset_holidays AS
(
  -- this is a copy of twin query in the feature_store folder
SELECT
  d.*
FROM dmart_order_forecast.dataset_holiday_periods d
WHERE 
  d.country_code = '{{params.country_code}}'
ORDER BY country_code, city_id, date

)
SELECT
  COUNTIF(city_id IS NULL) = 0 AS city_id_not_null,
  COUNTIF(city_id NOT BETWEEN 0 AND 10000) = 0 AS city_id_range,
  COUNTIF(date IS NULL) = 0 AS date_not_null,
  COUNTIF(date NOT BETWEEN '2000-01-01' AND '3000-01-01') = 0 AS date_range,
  COUNTIF(holiday_period_name IS NULL) = 0 AS holiday_period_name_not_null,
  COUNT(DISTINCT CONCAT(CAST(country_code AS STRING), CAST(city_id AS STRING), CAST(date AS STRING))) = COUNT(*) AS primary_key_condition
FROM dataset_holidays
