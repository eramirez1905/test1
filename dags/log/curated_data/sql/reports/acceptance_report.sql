CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.acceptance_report`
PARTITION BY report_date AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 6 WEEK) AS start_date
), batches AS (
  SELECT country_code
    , rider_id
    , b.number AS batch_number
    , b.active_from
    , b.active_until
  FROM `{{ params.project_id }}.cl.riders` r
  LEFT JOIN UNNEST(batches) b
), dataset AS (
  SELECT o.country_code
  , o.timezone
  , o.city_id
  , o.zone_id
  , o.order_id
  , o.order_status
  , o.created_at
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
    , t.rider_id
    , b.batch_number
    , (SELECT type FROM UNNEST(contracts) WHERE status = 'VALID' AND CAST(start_at AS DATE) <= '{{ next_ds }}' ORDER BY end_at DESC LIMIT 1) AS contract_type
    , t.state
  FROM dataset o
  LEFT JOIN UNNEST(deliveries) AS d
  LEFT JOIN UNNEST(d.transitions) AS t
  LEFT JOIN `{{ params.project_id }}.cl.riders` r ON o.country_code = r.country_code
    AND r.rider_id = t.rider_id
  LEFT JOIN batches b ON b.country_code = o.country_code
    AND b.rider_id = t.rider_id
    AND t.created_at BETWEEN b.active_from AND b.active_until
  WHERE t.state IN ('courier_notified', 'accepted')
), transitions_agg AS (
  SELECT t.country_code
    , t.timezone
    , t.city_id
    , t.zone_id
    , t.report_date_local
    , t.time_local
    , t.order_id
    , t.rider_id
    , t.batch_number
    , t.contract_type
    , COUNTIF(t.state = 'courier_notified') > 1 AS not_directly_accepted
    , COUNTIF(t.state = 'courier_notified') AS courier_notified
    , COUNTIF(t.state = 'accepted') AS courier_accepted
  FROM transitions t
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
), transitions_final AS (
  SELECT t.country_code
    , t.timezone
    , t.city_id
    , t.zone_id
    , t.report_date_local
    , t.time_local
    , t.rider_id
    , t.batch_number
    , t.contract_type
    , COUNTIF(t.not_directly_accepted IS TRUE) AS not_directly_accepted
    , COUNTIF(t.not_directly_accepted IS FALSE) AS directly_accepted
    , SUM(t.courier_notified) AS courier_notified
    , SUM(t.courier_accepted) AS courier_accepted
  FROM transitions_agg t
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
), payments AS (
  SELECT p.country_code
    , p.rider_id
    , b.delivery_id
    , b.created_at  AS delivery_accepted_at
    , MAX(IF(r.type = 'PER_DELIVERY', 'TRUE', 'FALSE')) AS has_payment_per_delivery
    , MAX(IF(r.type = 'PER_KM', 'TRUE', 'FALSE')) AS has_payment_per_km
    , MAX(IF(r.sub_type = 'PICKEDUP_DELIVERIES', 'TRUE', 'FALSE')) AS has_payment_pickedup_deliveries
    , MAX(IF(r.sub_type = 'NEAR_PICKUP_DELIVERIES', 'TRUE', 'FALSE')) AS has_payment_near_pickup_deliveries
    , MAX(IF(r.sub_type = 'COMPLETED_DELIVERIES', 'TRUE', 'FALSE')) AS has_payment_completed_deliveries
    , MAX(IF(r.sub_type = 'NEAR_DROPOFF_DELIVERIES', 'TRUE', 'FALSE')) AS has_payment_near_dropoff_deliveries
    , MAX(IF(r.sub_type = 'DROPOFF_DISTANCES', 'TRUE', 'FALSE')) AS has_payment_dropoff_distances
    , MAX(IF(r.sub_type = 'PICKEDUP_DISTANCES', 'TRUE', 'FALSE')) AS has_payment_pickedup_distances
    , MAX(IF(r.sub_type = 'ALL_DISTANCES', 'TRUE', 'FALSE')) AS has_payment_all_distances
    , SUM(b.total) AS total_payment
  FROM `{{ params.project_id }}.cl.rider_payments` p
  LEFT JOIN UNNEST (payment_details.basic) b ON b.status IN ('PENDING', 'PAID')
  LEFT JOIN `{{ params.project_id }}.cl.payments_basic_rules` r ON p.country_code = r.country_code
    AND b.payment_rule_id = r.id
  WHERE p.created_date >= (SELECT start_date FROM parameters)
    AND r.type IS NOT NULL
    AND r.sub_type IS NOT NULL
  GROUP BY 1, 2, 3, 4
), orders AS (
  SELECT o.country_code
    , o.timezone
    , o.city_id
    , o.zone_id
    , DATE(COALESCE(d.rider_dropped_off_at, o.created_at), o.timezone) AS report_date_local
    , TIME_TRUNC(CAST(DATETIME(COALESCE(d.rider_dropped_off_at, o.created_at), o.timezone) AS TIME), HOUR) AS time_local
    , d.rider_dropped_off_at
    , o.order_id
    , d.id AS delivery_id
    , d.rider_id
    , p.has_payment_per_delivery
    , p.has_payment_per_km
    , p.has_payment_pickedup_deliveries
    , p.has_payment_near_pickup_deliveries
    , p.has_payment_completed_deliveries
    , p.has_payment_near_dropoff_deliveries
    , p.has_payment_dropoff_distances
    , p.has_payment_pickedup_distances
    , p.has_payment_all_distances
    , p.total_payment
    , IF(d.delivery_status = 'completed', o.order_id, NULL) AS order_completed
    , IF(o.order_status = 'cancelled', o.order_id, NULL) AS order_cancelled
    , IF(is_outlier_pickup_distance_manhattan IS FALSE, od.pickup_distance_manhattan, NULL) AS pickup_distance_manhattan_km
    , IF(is_outlier_dropoff_distance_manhattan IS FALSE, od.dropoff_distance_manhattan, NULL) AS dropoff_distance_manhattan_km
  FROM dataset o
  LEFT JOIN UNNEST(deliveries) AS d
  LEFT JOIN `{{ params.project_id }}.cl._outlier_deliveries` od ON o.country_code = od.country_code
    AND d.id = od.delivery_id
  LEFT JOIN payments p ON o.country_code = p.country_code
    AND d.id = p.delivery_id
), orders_agg AS (
  SELECT o.country_code
    , o.timezone
    , o.city_id
    , o.zone_id
    , o.report_date_local
    , o.time_local
    , o.rider_id
    -- Since the payment type is per orders and we need to aggregate on rider level, this part gets the max for every hour
    , MAX(o.has_payment_per_delivery) AS has_payment_per_delivery
    , MAX(o.has_payment_per_km) AS has_payment_per_km
    , MAX(o.has_payment_pickedup_deliveries) AS has_payment_pickedup_deliveries
    , MAX(o.has_payment_near_pickup_deliveries) AS has_payment_near_pickup_deliveries
    , MAX(o.has_payment_completed_deliveries) AS has_payment_completed_deliveries
    , MAX(o.has_payment_near_dropoff_deliveries) AS has_payment_near_dropoff_deliveries
    , MAX(o.has_payment_dropoff_distances) AS has_payment_dropoff_distances
    , MAX(o.has_payment_pickedup_distances) AS has_payment_pickedup_distances
    , MAX(o.has_payment_all_distances) AS has_payment_all_distances
    , SUM(o.total_payment) AS payment_sum
    , COUNT(o.total_payment) AS payment_count
    , SUM(o.pickup_distance_manhattan_km) AS pickup_distance_manhattan_km_sum
    , COUNT(o.pickup_distance_manhattan_km) AS pickup_distance_manhattan_km_count
    , SUM(o.dropoff_distance_manhattan_km) AS dropoff_distance_manhattan_km_sum
    , COUNT(o.dropoff_distance_manhattan_km) AS dropoff_distance_manhattan_km_count
    , COUNT(o.order_completed) AS orders_completed
    , COUNT(o.order_cancelled) AS orders_cancelled
    , (COUNT(o.order_completed) + COUNT(o.order_cancelled)) AS gross_orders
  FROM orders o
  GROUP BY 1, 2, 3, 4, 5, 6, 7
), issues AS (
  SELECT i.country_code
    , i.city_id
    , i.zone_id
    , i.timezone
    , DATE(i.created_at, i.timezone) AS report_date_local
    , TIME_TRUNC(CAST(DATETIME(i.created_at, i.timezone) AS TIME), HOUR) AS time_local
    , i.issue_category
    , i.issue_id
    , i.delivery_id
    , i.rider_id
  FROM `{{ params.project_id }}.cl.issues` i
  WHERE i.created_date >= (SELECT start_date FROM parameters)
    AND i.issue_category IN ('courier_decline', 'not_accepted_within_x_minutes')
), issues_agg AS (
  SELECT i.country_code
    , i.city_id
    , i.zone_id
    , i.timezone
    , i.report_date_local
    , i.time_local
    , i.rider_id
    , COUNTIF(i.issue_category = 'courier_decline') AS courier_decline
    , COUNTIF(i.issue_category = 'not_accepted_within_x_minutes') AS not_accepted_within_x_minutes
  FROM issues i
  GROUP BY 1, 2, 3, 4, 5, 6, 7
), dates AS (
  SELECT o.country_code
    , o.city_id
    , o.zone_id
    , o.timezone
    , o.report_date_local
    , o.time_local
    , o.rider_id
  FROM orders_agg o
  -- removing darkstore pickers to not impact utr as they do not have orders, only working hours
  WHERE o.country_code NOT LIKE '%dp%'

  UNION DISTINCT

  SELECT t.country_code
    , t.city_id
    , t.zone_id
    , t.timezone
    , t.report_date_local
    , t.time_local
    , t.rider_id
  FROM transitions_final t
  -- removing darkstore pickers to not impact utr as they do not have orders, only working hours
  WHERE t.country_code NOT LIKE '%dp%'

  UNION DISTINCT

  SELECT i.country_code
    , i.city_id
    , i.zone_id
    , i.timezone
    , i.report_date_local
    , i.time_local
    , i.rider_id
  FROM issues_agg i
  -- removing darkstore pickers to not impact utr as they do not have orders, only working hours
  WHERE i.country_code NOT LIKE '%dp%'
)
SELECT d.country_code
  , c.country_name
  , c.region
  , d.city_id
  , cc.name AS city_name
  , d.zone_id
  , z.name AS zone_name
  , d.timezone
  , d.report_date_local AS report_date
  , CAST(d.time_local AS STRING) AS report_time
  , d.rider_id
  , t.courier_notified
  , t.courier_accepted
  , t.batch_number
  , t.contract_type
  , i.not_accepted_within_x_minutes
  , o.has_payment_per_delivery
  , o.has_payment_per_km
  , o.has_payment_pickedup_deliveries
  , o.has_payment_near_pickup_deliveries
  , o.has_payment_completed_deliveries
  , o.has_payment_near_dropoff_deliveries
  , o.has_payment_dropoff_distances
  , o.has_payment_pickedup_distances
  , o.has_payment_all_distances
  , o.payment_sum
  , o.payment_count
  , o.pickup_distance_manhattan_km_sum
  , o.pickup_distance_manhattan_km_count
  , SAFE_DIVIDE(o.pickup_distance_manhattan_km_sum, o.pickup_distance_manhattan_km_count) AS pickup_distance_manhattan_km_avg
  , o.dropoff_distance_manhattan_km_sum
  , o.dropoff_distance_manhattan_km_count
  , SAFE_DIVIDE(o.dropoff_distance_manhattan_km_sum, o.dropoff_distance_manhattan_km_count) AS dropoff_distance_manhattan_km_avg
  , o.orders_completed
  , o.orders_cancelled
  , o.gross_orders
FROM dates d
LEFT JOIN orders_agg o ON d.country_code = o.country_code
  AND d.rider_id = o.rider_id
  AND d.city_id = o.city_id
  AND d.zone_id = o.zone_id
  AND d.report_date_local = o.report_date_local
  AND d.time_local = o.time_local
LEFT JOIN transitions_final t ON d.country_code = t.country_code
  AND d.rider_id = t.rider_id
  AND d.city_id = t.city_id
  AND d.zone_id = t.zone_id
  AND d.report_date_local = t.report_date_local
  AND d.time_local = t.time_local
LEFT JOIN issues_agg i ON d.country_code = i.country_code
  AND d.rider_id = i.rider_id
  AND d.city_id = i.city_id
  AND d.zone_id = i.zone_id
  AND d.report_date_local = i.report_date_local
  AND d.time_local = i.time_local
LEFT JOIN `{{ params.project_id }}.cl.countries` c ON d.country_code = c.country_code
LEFT JOIN UNNEST(cities) cc ON d.city_id = cc.id
LEFT JOIN unnest(zones) z ON d.zone_id = z.id
WHERE t.report_date_local < '{{ next_ds }}'
