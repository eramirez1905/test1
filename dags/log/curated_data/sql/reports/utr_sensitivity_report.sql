CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.utr_sensitivity_report`
PARTITION BY report_date AS
WITH parameters AS (
  SELECT DATE_SUB('{{next_ds}}', INTERVAL 4 WEEK) AS start_date
), temp_delays AS (
  SELECT f.country_code
   , ci.id AS city_id
   , zz.id AS zone_id
   , DATE_TRUNC(CAST(f.created_at AS DATE), DAY) AS zone_stat_date
   , TIMESTAMP_TRUNC(CAST(f.created_at AS TIMESTAMP), MINUTE) AS zone_stat_time
   , AVG(f.delay) AS delay
   , AVG(utr) AS utr
  FROM `{{ params.project_id }}.ml.hurrier_zone_stats` f
  LEFT JOIN `{{ params.project_id }}.cl.countries` z ON z.country_code = f.country_code
  LEFT JOIN UNNEST(cities) ci
  LEFT JOIN UNNEST(zones) zz ON zz.id = f.geo_zone_id
  WHERE f.created_date >= (SELECT start_date FROM parameters)
  GROUP BY 1, 2, 3, 4, 5
), temp_deliveries AS (
  SELECT o.country_code
    , co.country_name
    , o.entity.display_name AS entity_display_name
    , d.city_id
    , ci.name AS city_name
    , d.timezone
    , o.zone_id
    , zz.name AS zone_name
    , d.id AS delivery_id
    , o.is_preorder AS preorder
    , d.vehicle.profile AS vehicle_type
    , ROUND(d.timings.rider_accepting_time / 60, 1) AS accepting_time
    , ROUND(d.timings.rider_reaction_time / 60, 1) AS reaction_time
    , ROUND(d.timings.to_vendor_time / 60, 1) AS to_vendor
    , ROUND(d.timings.at_vendor_time / 60, 1) AS at_vendor
    , ROUND(d.timings.to_customer_time / 60, 1) AS to_customer
    , ROUND(d.timings.at_customer_time / 60, 1) AS at_customer
    , ROUND(d.timings.actual_delivery_time / 60, 1) AS delivery_time
    , ROUND(o.timings.promised_delivery_time / 60, 1) AS expected_delivery_time
    , ROUND(IF(o.is_preorder IS FALSE, o.timings.hold_back_time / 60, 1)) AS hold_back_time
    , ROUND(d.timings.delivery_delay / 60, 1) AS delivery_delay
    , ROUND(d.timings.dispatching_time / 60, 1) AS dispatching_time
    , ROUND(d.timings.vendor_late / 60, 1) AS vendor_late
    , ROUND(d.timings.rider_late / 60, 1) AS courier_late
    , ROUND(d.delivery_distance / 1000, 1) AS delivery_distance
    , ROUND(d.pickup_distance_manhattan / 1000, 1) AS pickup_distance
    , ROUND(d.dropoff_distance_manhattan / 1000, 1) AS dropoff_distance
    , CAST(DATETIME(o.created_at, d.timezone) AS DATE) AS created_date
    , FORMAT_DATE('%G-%V', (CAST(DATETIME(d.rider_dropped_off_at, d.timezone) AS DATE))) AS delivery_week
    , o.created_at
  FROM `{{ params.project_id }}.cl.orders` AS o
  LEFT JOIN UNNEST(deliveries) d
  LEFT JOIN `{{ params.project_id }}.cl.countries` AS co ON o.country_code = co.country_code
  LEFT JOIN UNNEST(cities) ci ON d.city_id = ci.id
  LEFT JOIN UNNEST(zones) zz ON o.zone_id = zz.id
  WHERE o.created_date >= (SELECT start_date FROM parameters)
    AND d.delivery_status = 'completed'
    AND d.is_redelivery = FALSE
    AND d.timings.dispatching_time / 60 <= 120
    AND d.timings.rider_reaction_time / 60 <= 30
    AND d.timings.rider_accepting_time / 60 <= 60
    AND d.timings.to_vendor_time / 60 <= 35
    AND d.timings.at_vendor_time / 60 <= 35
    AND d.timings.to_customer_time / 60 <= 35
    AND d.timings.at_customer_time / 60 <= 20
    AND d.timings.actual_delivery_time / 60 <= 90
    AND o.timings.promised_delivery_time / 60 <= 90
    AND d.timings.delivery_delay / 60 <= 45
    AND d.delivery_distance / 1000 <= 6
    AND COALESCE(d.pickup_distance_google, d.pickup_distance_manhattan) / 1000 <= 6
    AND COALESCE(d.dropoff_distance_google, d.dropoff_distance_manhattan) / 1000 <= 6
)
SELECT ops.country_code
  , ops.country_name
  , ops.entity_display_name
  , ops.city_id
  , ops.city_name
  , ops.zone_id
  , ops.zone_name
  , ops.delivery_id
  , ops.preorder
  , ops.dispatching_time
  , ops.reaction_time
  , ops.accepting_time
  , ops.to_vendor
  , ops.at_vendor
  , ops.to_customer
  , ops.at_customer
  , ops.delivery_time
  , ROUND(CAST(ops.expected_delivery_time AS FLOAT64), 0) AS expected_delivery_time
  , ops.hold_back_time
  , ops.delivery_delay
  , ops.delivery_distance
  , ops.pickup_distance
  , ops.dropoff_distance
  , ops.created_date AS report_date
  , ops.delivery_week
  , ROUND(CAST(del.utr AS FLOAT64), 1) AS live_utr
  , ROUND(CAST(del.delay AS FLOAT64), 0) AS delay_by_zone
FROM temp_deliveries AS ops
LEFT JOIN temp_delays AS del ON ops.country_code = del.country_code
  AND ops.city_id = del.city_id
  AND ops.zone_id = del.zone_id
  AND ops.created_date = del.zone_stat_date
  AND CAST(TIMESTAMP_TRUNC(ops.created_at, MINUTE) AS TIME) = CAST(del.zone_stat_time AS TIME)
WHERE del.utr <= 4
  AND del.utr > 0
  -- removing darkstore pickers to not impact utr as they do not have orders, only working hours
  AND ops.country_code NOT LIKE '%dp%'
;
