WITH
dataset_zone_hour AS
(
  -- this is a copy of feature_store/dataset_zone_hour.sql
  SELECT
    d.*
  FROM forecasting.dataset_zone_hour d
  LEFT JOIN forecasting.zones z USING (country_code, zone_id)
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
  COUNTIF(orders_lost_net IS NULL) = 0 AS orders_net_not_null,
  COUNTIF(orders_lost_net NOT BETWEEN 0 AND 1E5) = 0 AS orders_lost_net_range,
  COUNTIF(orders_close_lost_net IS NULL) = 0 AS orders_close_lost_net_not_null,
  COUNTIF(orders_close_lost_net NOT BETWEEN 0 AND 1E5) = 0 AS orders_close_lost_net_range,
  COUNTIF(orders_shrink_lost_net IS NULL) = 0 AS orders_shrink_lost_net_not_null,
  COUNTIF(orders_shrink_lost_net NOT BETWEEN 0 AND 1E5) = 0 AS orders_shrink_lost_net_range,
  COUNTIF(tag_halal IS NULL) = 0 AS tag_halal_not_null,
  COUNTIF(tag_halal NOT BETWEEN 0 AND orders_all) = 0 AS tag_halal_range,
  COUNTIF(tag_preorder IS NULL) = 0 AS tag_preorder_no_null,
  COUNTIF(tag_preorder NOT BETWEEN 0 AND orders_all) = 0 AS tag_preorder_range,
  COUNTIF(tag_corporate IS NULL) = 0 AS tag_corporate_not_null,
  COUNTIF(tag_corporate NOT BETWEEN 0 AND orders_all) = 0 AS tag_corporate_range,
  COUNTIF(event_close_value_mean IS NULL) = 0 AS event_close_value_mean_not_null,
  COUNTIF(event_close_value_mean NOT BETWEEN 0 AND 1) = 0 AS event_close_value_mean_range,
  COUNTIF(event_close_duration IS NULL) = 0 AS event_close_duration_not_null,
  COUNTIF(event_close_duration NOT BETWEEN 0 AND 30) = 0 AS event_close_duration_range,
  COUNTIF(event_shrink_value_mean IS NULL) = 0 AS event_shrink_value_mean_not_null,
  COUNTIF(event_shrink_value_mean NOT BETWEEN 0 AND 50) = 0 AS event_shrink_value_mean_range,
  COUNTIF(event_shrink_value_median IS NOT NULL AND event_shrink_value_median NOT BETWEEN 0 AND 60) = 0 AS event_shrink_value_median_range,
  COUNTIF(event_shrink_duration IS NULL) = 0 AS event_shrink_value_duration_not_null,
  COUNTIF(event_shrink_duration NOT BETWEEN 0 AND 30) = 0 AS event_shrink_value_duration_range,
  COUNTIF(event_shrink_legacy_value_mean IS NULL) = 0 AS event_shrink_legacy_value_mean_not_null,
  COUNTIF(event_shrink_legacy_duration IS NULL) = 0 AS event_shrink_legacy_value_duration_not_null,
  COUNTIF(event_shrink_legacy_duration NOT BETWEEN 0 AND 30) = 0 AS event_shrink_legacy_value_duration_range,
  COUNTIF(event_delay_value_mean IS NULL) = 0 AS event_delay_value_mean_not_null,
  COUNTIF(event_delay_value_mean NOT BETWEEN -5 AND 120) = 0 AS event_delay_value_mean_range,
  COUNTIF(event_outage_value_mean IS NULL) = 0 AS event_outage_value_mean_not_null,
  COUNTIF(event_outage_value_mean NOT BETWEEN 0 AND 1) = 0 AS event_outage_value_mean_range,
  COUNT(DISTINCT CONCAT(CAST(country_code AS STRING), CAST(zone_id AS STRING), CAST(datetime AS STRING))) = COUNT(*) AS primary_key_condition,
  -- more intelligent order checks:
  CASE WHEN SUM(orders) > 0 THEN SUM(CASE WHEN zone_id = 0 THEN orders ELSE 0 END) / SUM(orders) < 0.02 ELSE TRUE END AS orders_zone_zero_tolerance,
  SUM(CASE WHEN zone_id > 0 THEN orders ELSE 0 END) > 0 AS orders_zone_nonzero_exist,
  SUM(CASE WHEN zone_id > 0 THEN orders ELSE 0 END) > SUM(CASE WHEN zone_id = 0 THEN orders ELSE 0 END) AS orders_zone_zero_lower,
    -- more sensible checks:
  CASE
    WHEN ANY_VALUE(country_code) IN ('sg', 'at', 'ar') THEN AVG(event_close_value_mean) > 0
    ELSE TRUE
  END AS event_close_value_mean_avg,
  CASE
    -- there should be more than a minute of shrinking per day on avg
    WHEN ANY_VALUE(country_code) IN ('sg', 'at', 'ar') THEN AVG(event_shrink_duration) > 1/3600
    ELSE TRUE
  END AS event_shrink_duration_avg,
  CASE
    -- slow growers
    WHEN ANY_VALUE(country_code) IN ('at', 'no', 'ca') THEN AVG(orders) BETWEEN 1 / 48 AND 20000 / 48
    -- fast growers
    WHEN ANY_VALUE(country_code) IN ('tw', 'sa', 'ar') THEN AVG(orders) BETWEEN 1 AND 1E6 / 48
    ELSE TRUE
  END AS orders_avg,
  CASE
    -- open during normal business hours
    WHEN ANY_VALUE(country_code) IN ('at', 'cz') THEN AVG(CASE WHEN orders > 0 THEN 1 ELSE 0 END) BETWEEN 0.1 AND 0.9
    -- open almost 24/7
    WHEN ANY_VALUE(country_code) IN ('kw', 'sg') THEN AVG(CASE WHEN orders > 0 THEN 1 ELSE 0 END) BETWEEN 0.4 AND 0.99
    ELSE TRUE
  END AS orders_not_zero_avg
FROM dataset_zone_hour
