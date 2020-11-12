CREATE TEMPORARY FUNCTION get_created_date(X ANY TYPE) AS
(
  DATE_TRUNC(CAST(X AS DATE), ISOWEEK)
); 

CREATE OR REPLACE TABLE dmart_order_forecast.dataset_zone_hour
PARTITION BY created_date
CLUSTER BY country_code, zone_id AS
WITH dataset_zone_hour AS
(
  SELECT
    t.country_code,
    t.zone_id,
    t.city_id,
    t.datetime,
    t.timezone,
    COALESCE(o.orders, 0) AS orders, 
    COALESCE(o.orders_all, 0) AS orders_all, 
    COALESCE(o.orders_completed, 0) AS orders_completed,
    COALESCE(o.orders_cancelled, 0) AS orders_cancelled,
    COALESCE(o.orders_dispatcher_cancelled, 0) AS orders_dispatcher_cancelled,
    -- TODO: fix these
    0 AS orders_lost_net,
    0 AS orders_close_lost_net,
    0 AS orders_shrink_lost_net,
    COALESCE(o.tag_halal, 0) AS tag_halal,
    COALESCE(o.tag_preorder, 0) AS tag_preorder,
    COALESCE(o.tag_corporate, 0) AS tag_corporate,
    0 AS event_close_value_mean,
    0 AS event_close_duration,
    0 AS event_shrink_value_mean,
    -- if no (absolute) shrinking is active, we should not coalesce the weighted mean to 0 
    -- but rather use median (which is NA when no shrinking was active) & duration
    NULL AS event_shrink_value_median,
    0 AS event_shrink_duration,
    0 AS event_shrink_legacy_value_mean,
    0 AS event_shrink_legacy_duration,
    0 AS event_delay_value_mean,
    0 AS event_outage_value_mean,
    NULL AS adjustment_reason,
    FALSE AS is_adjusted
  FROM `dmart_order_forecast.timeseries` t
  LEFT JOIN `dmart_order_forecast.orders_timeseries` o
    ON o.country_code = t.country_code
    AND o.zone_id = t.zone_id
    AND o.datetime = t.datetime
)
SELECT
  *,
  get_created_date(datetime) AS created_date
FROM dataset_zone_hour
WHERE 
  datetime <= '{{next_execution_date}}'
