CREATE TEMPORARY FUNCTION get_created_date(X ANY TYPE) AS
(
  DATE_TRUNC(CAST(X AS DATE), ISOWEEK)
); 

CREATE OR REPLACE TABLE forecasting.dataset_zone_hour
PARTITION BY created_date
CLUSTER BY country_code AS
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
    COALESCE(orders_lost.orders_lost_net, 0) AS orders_lost_net,
    COALESCE(orders_lost.orders_close_lost_net, 0) AS orders_close_lost_net,
    COALESCE(orders_lost.orders_shrink_lost_net, 0) AS orders_shrink_lost_net,
    COALESCE(o.tag_halal, 0) AS tag_halal,
    COALESCE(o.tag_preorder, 0) AS tag_preorder,
    COALESCE(o.tag_corporate, 0) AS tag_corporate,
    COALESCE(events_close.value_mean, 0) AS event_close_value_mean,
    COALESCE(events_close.duration, 0) AS event_close_duration,
    COALESCE(events_shrink.value_mean, 0) AS event_shrink_value_mean,
    -- if no (absolute) shrinking is active, we should not coalesce the weighted mean to 0 
    -- but rather use median (which is NA when no shrinking was active) & duration
    events_shrink.value_median AS event_shrink_value_median,
    COALESCE(events_shrink.duration, 0) AS event_shrink_duration,
    COALESCE(events_shrink_legacy.value_mean, 0) AS event_shrink_legacy_value_mean,
    COALESCE(events_shrink_legacy.duration, 0) AS event_shrink_legacy_duration,
    COALESCE(events_delay.value_mean, 0) AS event_delay_value_mean,
    COALESCE(events_outage.value_mean, 0) AS event_outage_value_mean,
    adjustments.reason AS adjustment_reason,
    adjustments.reason IS NOT NULL AS is_adjusted
  FROM `forecasting.timeseries` t
  LEFT JOIN `forecasting.orders_timeseries` o
    ON o.country_code = t.country_code
    AND o.zone_id = t.zone_id
    AND o.datetime = t.datetime
  LEFT JOIN (
      SELECT
        country_code,
        zone_id,
        datetime,
        -- do not include outage lost orders as those events are not necessarily disjoint with close events, 
        -- they are eventually handled by ts imputation
        SUM(IF(event_type IN ('close', 'shrink', 'shrink_legacy'), orders_lost_net, 0)) AS orders_lost_net,
        SUM(IF(event_type IN ('close'), orders_lost_net, 0)) AS orders_close_lost_net,
        SUM(IF(event_type IN ('shrink', 'shrink_legacy'), orders_lost_net, 0)) AS orders_shrink_lost_net
      FROM `forecasting.orders_lost_timeseries`
      GROUP BY 1,2,3
    ) orders_lost
    ON orders_lost.country_code = t.country_code
    AND orders_lost.zone_id = t.zone_id
    AND orders_lost.datetime = t.datetime
  LEFT JOIN `forecasting.events_timeseries` events_close
    ON events_close.country_code = t.country_code
    AND events_close.zone_id = t.zone_id
    AND events_close.datetime = t.datetime
    AND events_close.event_type = 'close'
  LEFT JOIN `forecasting.events_timeseries` events_shrink
    ON events_shrink.country_code = t.country_code
    AND events_shrink.zone_id = t.zone_id
    AND events_shrink.datetime = t.datetime
    AND events_shrink.event_type = 'shrink'
  LEFT JOIN `forecasting.events_timeseries` events_shrink_legacy
    ON events_shrink_legacy.country_code = t.country_code
    AND events_shrink_legacy.zone_id = t.zone_id
    AND events_shrink_legacy.datetime = t.datetime
    AND events_shrink_legacy.event_type = 'shrink_legacy'
  LEFT JOIN `forecasting.events_timeseries` events_delay
    ON events_delay.country_code = t.country_code
    AND events_delay.zone_id = t.zone_id
    AND events_delay.datetime = t.datetime
    AND events_delay.event_type = 'delay'
  LEFT JOIN `forecasting.events_timeseries` events_outage
    ON events_outage.country_code = t.country_code
    AND events_outage.zone_id = t.zone_id
    AND events_outage.datetime = t.datetime
    AND events_outage.event_type = 'outage'
  LEFT JOIN `forecasting.adjustments_timeseries` adjustments
    ON adjustments.country_code = t.country_code
    AND adjustments.zone_id = t.zone_id
    AND adjustments.datetime = t.datetime
)
SELECT
  *,
  get_created_date(datetime) AS created_date
FROM dataset_zone_hour
WHERE 
  datetime <= TIMESTAMP('{{next_execution_date}}')
