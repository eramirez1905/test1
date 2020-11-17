WITH
dataset_zone_hour AS
(
  -- this is a copy of feature_store/dataset_zone_hour.sql
  SELECT
    d.*
  FROM dmart_order_forecast.dataset_zone_hour d
  LEFT JOIN dmart_order_forecast.zones z USING (country_code, zone_id)
  WHERE 
    d.country_code = '{{params.country_code}}'
    -- only load data before midnight
    AND d.datetime < z.datetime_end_midnight
)
SELECT
  COUNTIF(zone_id IS NULL) = 0 AS zone_id_not_null,
  COUNTIF(zone_id NOT BETWEEN 0 AND 1E10) = 0 AS zone_id_range,
  COUNTIF(city_id IS NULL) = 0 AS city_id_not_null,
  COUNTIF(city_id NOT BETWEEN 0 AND 1E10) = 0 AS city_id_range,
  COUNTIF(datetime IS NULL) = 0 AS datetime_not_null,
  -- -- approx. date of first hurrier migration
  COUNTIF(datetime NOT BETWEEN '2015-12-01 00:00:00' AND CURRENT_TIMESTAMP()) = 0 AS datetime_range,
  -- use SAFE.DATETIME such that a corrupted datetime or timezone entry will lead to 
  -- nulls instead of the failure of the function.
  COUNTIF(SAFE.DATETIME(datetime, timezone) IS NULL) = 0 AS timezone_conversion,
  COUNTIF(orders IS NULL) = 0 AS orders_not_null,
  COUNTIF(orders NOT BETWEEN 0 AND orders_all) = 0 AS orders_range,
  COUNTIF(orders_all IS NULL) = 0 AS orders_all_not_null,
  COUNTIF(orders_all NOT BETWEEN 0 AND 1E5) = 0 AS orders_all_range,
  COUNTIF(orders_completed IS NULL) = 0 AS orders_completed_not_null,
  COUNTIF(orders_completed NOT BETWEEN 0 AND orders_all) = 0 AS orders_completed_range,
  COUNTIF(orders_cancelled IS NULL) = 0 AS orders_cancelled_not_null,
  COUNTIF(orders_cancelled NOT BETWEEN 0 AND orders_all) = 0 AS orders_cancelled_range,
  COUNTIF(orders_dispatcher_cancelled IS NULL) = 0 AS orders_dispatcher_cancelled_not_null,
  COUNTIF(orders_dispatcher_cancelled NOT BETWEEN 0 AND orders_all) = 0 AS orders_dispatcher_cancelled_range,
  -- TODO: add checks
  COUNT(DISTINCT CONCAT(CAST(country_code AS STRING), CAST(zone_id AS STRING), CAST(datetime AS STRING))) = COUNT(*) AS primary_key_condition,
  -- more intelligent order checks:
  SUM(CASE WHEN zone_id > 0 THEN orders ELSE 0 END) > 0 AS orders_zone_nonzero_exist,
FROM dataset_zone_hour
