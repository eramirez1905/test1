CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.acceptance_report_order_level`
PARTITION BY report_date AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 6 WEEK) AS start_date
), issues AS (
  SELECT i.country_code
    , i.city_id
    , i.zone_id
    , i.timezone
    , DATE(i.created_at, i.timezone) AS report_date_local
    , TIME_TRUNC(CAST(DATETIME(i.created_at, i.timezone) AS TIME), HOUR) AS time_local
    , COUNT(DISTINCT issue_id) AS courier_decline
  FROM `{{ params.project_id }}.cl.issues` i
  WHERE i.created_date >= (SELECT start_date FROM parameters)
    AND (i.issue_category = 'courier_decline' OR i.issue_type = 'Issue::DispatchCourierDecline')
  GROUP BY 1, 2, 3, 4, 5, 6
), dataset AS (
  SELECT o.country_code
  , o.timezone
  , o.city_id
  , o.zone_id
  , o.order_id
  , o.order_status
  , o.created_at
  , IF(o.cod_collect_at_dropoff = 0, 'True', 'False') AS is_pay_online
  , deliveries
  , cancellation
  FROM `{{ params.project_id }}.cl.orders` o
  WHERE o.created_date >= (SELECT start_date FROM parameters)
), transitions AS (
  SELECT o.country_code
    , o.timezone
    , o.city_id
    , o.zone_id
    , DATE(COALESCE(d.rider_dropped_off_at, o.created_at), o.timezone) AS report_date_local
    , TIME_TRUNC(CAST(DATETIME(COALESCE(d.rider_dropped_off_at, o.created_at), o.timezone) AS TIME), HOUR) AS time_local
    , o.order_id
    , d.id AS delivery_id
    , d.rider_notified_at
    , o.is_pay_online
    , o.created_at
    , t.state
    , IF(d.delivery_status = 'completed', o.order_id, NULL) AS order_completed
    , IF(o.order_status = 'cancelled', o.order_id, NULL) AS order_cancelled
    , IF(o.cancellation.reason IN ('TRAFFIC_MANAGER_NO_RIDER', 'NO_COURIER') AND o.cancellation.source = 'issue_service' AND o.order_status = 'cancelled', o.order_id, NULL) AS order_failed_to_assign
  FROM dataset o
  LEFT JOIN UNNEST(deliveries) AS d
  LEFT JOIN UNNEST(d.transitions) AS t
  WHERE t.state IN ('courier_notified', 'accepted')
), transitions_agg AS (
  SELECT t.country_code
    , t.timezone
    , t.city_id
    , t.zone_id
    , t.report_date_local
    , t.time_local
    , t.order_id
    , t.is_pay_online
    , IF(is_outlier_pickup_distance_manhattan IS FALSE, od.pickup_distance_manhattan, NULL) AS pickup_distance_manhattan_km
    , IF(is_outlier_dropoff_distance_manhattan IS FALSE, od.dropoff_distance_manhattan, NULL) AS dropoff_distance_manhattan_km
    , TIMESTAMP_DIFF(t.rider_notified_at, t.created_at, SECOND) AS time_to_notify
    , COUNTIF(t.state = 'courier_notified') > 1 AS not_directly_accepted
    , COUNTIF(t.state = 'courier_notified') = 2 AS notified_2
    , COUNTIF(t.state = 'courier_notified') = 3 AS notified_3
    , COUNTIF(t.state = 'courier_notified') = 4 AS notified_4
    , COUNTIF(t.state = 'courier_notified') > 4 AS notified_5_plus
    , COUNTIF(t.state = 'courier_notified') AS courier_notified
    , COUNTIF(t.state = 'accepted') AS courier_accepted
    , COUNT(DISTINCT t.order_completed) AS orders_completed
    , COUNT(DISTINCT t.order_cancelled) AS orders_cancelled
    , (COUNT(DISTINCT t.order_completed) + COUNT(DISTINCT t.order_cancelled)) AS gross_orders
    , COUNT(DISTINCT t.order_failed_to_assign) AS order_failed_to_assign
  FROM transitions t
  LEFT JOIN `{{ params.project_id }}.cl._outlier_deliveries` od ON t.country_code = od.country_code
    AND t.delivery_id = od.delivery_id
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
)
SELECT t.country_code
  , c.country_name
  , c.region
  , t.city_id
  , cc.name AS city_name
  , t.zone_id
  , z.name AS zone_name
  , t.report_date_local AS report_date
  , CAST(t.time_local AS STRING) AS report_time
  , i.courier_decline
  , t.is_pay_online
  , AVG(t.time_to_notify) AS time_to_notify_avg_sec
  , SUM(t.pickup_distance_manhattan_km) AS pickup_distance_manhattan_km_sum
  , COUNT(t.pickup_distance_manhattan_km) AS pickup_distance_manhattan_km_count
  , SUM(t.dropoff_distance_manhattan_km) AS dropoff_distance_manhattan_km_sum
  , COUNT(t.dropoff_distance_manhattan_km) AS dropoff_distance_manhattan_km_count
  , SAFE_DIVIDE(SUM(t.dropoff_distance_manhattan_km), COUNT(t.dropoff_distance_manhattan_km)) AS dropoff_distance_manhattan_km_avg
  , SAFE_DIVIDE(SUM(t.pickup_distance_manhattan_km), COUNT(t.pickup_distance_manhattan_km)) AS pickup_distance_manhattan_km_avg
  , COUNTIF(t.not_directly_accepted IS TRUE) AS not_directly_accepted
  , COUNTIF(t.not_directly_accepted IS FALSE) AS directly_accepted
  , COUNTIF(t.notified_2 IS TRUE) as notified_2
  , COUNTIF(t.notified_3 IS TRUE) as notified_3
  , COUNTIF(t.notified_4 IS TRUE) as notified_4
  , COUNTIF(t.notified_5_plus IS TRUE) as notified_5_plus
  , SUM(t.courier_notified) AS courier_notified
  , SUM(t.courier_accepted) AS courier_accepted
  , SUM(t.orders_completed) AS orders_completed
  , SUM(t.orders_cancelled) AS orders_cancelled
  , SUM(t.gross_orders) AS gross_orders
  , SUM(t.order_failed_to_assign) AS orders_failed_to_assign
FROM transitions_agg t
LEFT JOIN issues i ON t.country_code = i.country_code
  AND t.city_id = i.city_id
  AND t.zone_id = i.zone_id
  AND t.report_date_local = i.report_date_local
  AND t.time_local = i.time_local
LEFT JOIN `{{ params.project_id }}.cl.countries` c ON t.country_code = c.country_code
LEFT JOIN UNNEST(cities) cc ON t.city_id = cc.id
LEFT JOIN unnest(zones) z ON t.zone_id = z.id
WHERE t.report_date_local < '{{ next_ds }}'
 -- removing darkstore pickers to not impact utr as they do not have orders, only working hours
  AND t.country_code NOT LIKE '%dp%'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
