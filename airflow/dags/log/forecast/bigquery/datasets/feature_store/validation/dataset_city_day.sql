WITH
dataset_city_day AS
(
  -- this is a copy of feature_store/dataset_city_day.sql
  WITH cities AS
  (
    SELECT
      country_code,
      city_id,
      MIN(DATE(DATETIME(datetime_end_midnight, timezone))) AS date_end
    FROM forecasting.zones
    GROUP BY 1,2
  )
  SELECT
    d.*
  FROM forecasting.dataset_city_day d
  LEFT JOIN cities c USING (country_code, city_id)
  WHERE 
    d.country_code = '{{params.country_code}}'
    AND d.date < c.date_end
  ORDER BY country_code, city_id, date
)
SELECT
  COUNTIF(city_id IS NULL) = 0 AS city_id_not_null,
  COUNTIF(city_id NOT BETWEEN 0 AND 1E10) = 0 AS city_id_range,
  COUNTIF(date IS NULL) = 0 AS date_not_null,
  -- -- approx. date of first platform orders
  COUNTIF(date NOT BETWEEN '2014-01-01' AND CURRENT_DATE()) = 0 AS date_range,
  COUNTIF(orders IS NULL) = 0 AS orders_not_null,
  COUNTIF(orders NOT BETWEEN 0 AND 1E6) = 0 AS orders_range,
  COUNTIF(orders NOT BETWEEN 0 AND orders_all) = 0 AS orders_range_naive,
  COUNTIF(orders_completed IS NULL) = 0 AS orders_completed_not_null,
  COUNTIF(orders_completed NOT BETWEEN 0 AND orders_all) = 0 AS orders_completed_range,
  COUNTIF(orders_cancelled IS NULL) = 0 AS orders_cancelled_not_null,
  COUNTIF(orders_cancelled NOT BETWEEN 0 AND orders_all) = 0 AS orders_cancelled_range,
  COUNTIF(orders_lost_net IS NULL) = 0 AS orders_lost_net_not_null,
  COUNTIF(orders_lost_net NOT BETWEEN 0 AND 1E6) = 0 AS orders_lost_net_range,
  COUNTIF(orders_close_lost_net IS NULL) = 0 AS orders_close_lost_net_not_null,
  COUNTIF(orders_close_lost_net NOT BETWEEN 0 AND 1E5) = 0 AS orders_close_lost_net_range,
  COUNTIF(orders_shrink_lost_net IS NULL) = 0 AS orders_shrink_lost_net_not_null,
  COUNTIF(orders_shrink_lost_net NOT BETWEEN 0 AND 1E5) = 0 AS orders_shrink_lost_net_range,
  COUNT(DISTINCT CONCAT(CAST(country_code AS STRING), CAST(city_id AS STRING), CAST(date AS STRING))) = COUNT(*) AS primary_key_condition
FROM dataset_city_day
