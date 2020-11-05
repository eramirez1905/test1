CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._rider_report_source`
PARTITION BY report_date AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 6 WEEK) AS start_date
    , DATE('{{ next_ds }}') AS end_date
), dates AS (
SELECT listed_date
  , FORMAT_DATE('%G-%V', listed_date) AS week_number
FROM (
  SELECT GENERATE_DATE_ARRAY(start_date, '{{ next_ds }}', INTERVAL 1 DAY) AS date_array
  FROM parameters
  ), UNNEST(date_array) listed_date
), entities AS (
  SELECT co.country_code
    -- add a concatenation of all the platform in each country for visualization purposes.
    , ARRAY_TO_STRING(ARRAY_AGG(p.display_name IGNORE NULLS), ' / ') AS entities
  FROM `{{ params.project_id }}.cl.countries` co
  LEFT JOIN UNNEST (co.platforms) p
  -- remove legacy display name from report, as there is no data under it since 2017, however it may cause confusion to the user.
  WHERE p.display_name NOT IN ('FD - Bahrain')
  GROUP BY  1
), countries AS (
  SELECT co.country_code
    , co.country_name
    , platforms
    , ci.id AS city_id
    , ci.name AS city_name
    , z.id AS zone_id
    , z.name AS zone_name
  FROM `{{ params.project_id }}.cl.countries` co
  LEFT JOIN UNNEST(cities) ci
  LEFT JOIN UNNEST(ci.zones) z
), batches AS (
  SELECT country_code
    , rider_id
    , b.number AS batch_number
    , b.active_from
    , b.active_until
  FROM `{{ params.project_id }}.cl.riders` r
  LEFT JOIN UNNEST(batches) b
), temp_periods_base AS (
  SELECT country_code
    , city_id
    , zone_id
    , CASE
        WHEN lower(weekday) = 'monday'    THEN 1
        WHEN lower(weekday) = 'tuesday'   THEN 2
        WHEN lower(weekday) = 'wednesday' THEN 3
        WHEN lower(weekday) = 'thursday'  THEN 4
        WHEN lower(weekday) = 'friday'    THEN 5
        WHEN lower(weekday) = 'saturday'  THEN 6
        WHEN lower(weekday) = 'sunday'    THEN 7
     END AS weekday
    , start_time AS start_peak
    , end_time AS end_peak
    , period_name AS time_period
    , ROW_NUMBER() OVER (PARTITION BY country_code, zone_id, lower(weekday), start_time ORDER BY created_at DESC, u.updated_at DESC) AS ranking
  FROM `{{ params.project_id }}.cl.utr_target_periods`
  LEFT JOIN UNNEST(utr) u
  WHERE country_code != ''
    AND zone_id IS NOT NULL
), temp_periods AS (
  SELECT * EXCEPT(ranking)
  FROM temp_periods_base
  WHERE ranking = 1
), temp_shifts AS (
  SELECT s.country_code
    , s.shift_id
    , DATE(DATETIME(actual_start_at, timezone)) AS report_date
    , tp.start_peak
    , tp.end_peak
    , LEAST(CAST(end_peak AS TIME), IF(DATE(DATETIME(s.actual_end_at, s.timezone)) != DATE(DATETIME(s.actual_start_at, s.timezone)), '23:59:59', TIME(DATETIME(s.actual_end_at, s.timezone)))) AS least
    , GREATEST(CAST(start_peak AS TIME), CAST(TIMESTAMP(DATETIME(s.actual_start_at, s.timezone)) AS TIME)) AS greatest
  FROM `{{ params.project_id }}.cl.shifts` s
  LEFT JOIN temp_periods tp ON s.country_code = tp.country_code
      AND s.city_id = tp.city_id
      AND s.zone_id = tp.zone_id
      AND FORMAT_DATE("%u", CAST(DATETIME(s.actual_start_at, s.timezone) AS DATE)) = CAST(tp.weekday AS STRING)
      AND tp.time_period IN ('lunch', 'dinner', 'Lunch', 'Dinner')
      AND TIME(DATETIME(s.actual_start_at, s.timezone)) <= tp.end_peak
      AND tp.start_peak <= IF(DATE(DATETIME(s.actual_end_at, s.timezone)) != DATE(DATETIME(s.actual_start_at, s.timezone)), '23:59:59', TIME(DATETIME(s.actual_end_at, s.timezone)))
   WHERE s.created_date >= (SELECT start_date FROM parameters)
    AND s.shift_state = 'EVALUATED'
), temps_shifts_final AS (
  SELECT country_code
    , shift_id
    , report_date
    , COALESCE(
        IF(SUM(TIME_DIFF(least, greatest, SECOND) / 60) < 0,
          SUM(TIME_DIFF(end_peak, start_peak, SECOND) / 60),
          SUM(TIME_DIFF(least, greatest, SECOND) / 60)
        ), 0
      ) AS peak_time
  FROM temp_shifts
  GROUP BY 1, 2, 3
), shift_swap_requests AS (
  SELECT s.country_code
    , s.created_date
    , s.created_by
    , s.shift_id
    , sr.created_at
    , sr.status
    , sr.accepted_by
  FROM `{{ params.project_id }}.cl.shift_swap_requests` s
  LEFT JOIN UNNEST (swap_requests) sr
), swaps AS (
  SELECT sw.country_code
    , CAST(DATETIME(s.shift_start_at, s.timezone) AS DATE) AS start_date_local
    , s.rider_id
    , ROW_NUMBER() OVER (PARTITION BY sw.country_code, sw.shift_id, s.rider_id ORDER BY sw.created_at DESC) AS row_number ---- take last transition in swap history for each shift
    , s.shift_state
    , sw.status
    , sw.shift_id
  FROM shift_swap_requests sw
  LEFT JOIN `{{ params.project_id }}.cl.shifts` s ON sw.country_code = s.country_code
    AND sw.shift_id = s.shift_id
    AND s.created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 6 WEEK)
  WHERE sw.created_date >= (SELECT start_date FROM parameters)
), vehicle_dataset AS (
  SELECT country_code
    , rider_id
    , id
    , CAST(DATETIME(rider_dropped_off_at, o.timezone) AS DATE) AS report_date
    , vehicle.profile AS vehicle_profile
    , vehicle.name AS vehicle_name
  FROM `{{ params.project_id }}.cl.orders` o
  LEFT JOIN UNNEST (deliveries) d
  WHERE d.delivery_status = 'completed'
    AND o.created_date >= (SELECT start_date FROM parameters)
  ORDER BY country_code, rider_id, report_date, rider_dropped_off_at
), vehicle_most_orders_ordered AS (
  SELECT country_code
    , rider_id
    , report_date
    , vehicle_profile
    , vehicle_name
    , ROW_NUMBER() OVER (PARTITION BY country_code, rider_id, report_date ORDER BY COUNT(DISTINCT id) DESC) AS row_number
    , COUNT(id) AS deliveries
  FROM vehicle_dataset
  GROUP BY 1, 2, 3, 4, 5
), logs AS (
  SELECT a.country_code
    , o.entity.display_name AS entity
    , o.city_id
    , oz.zone_id
    , hurrier.manual_undispatch.old_rider_id AS rider_id
    , hurrier.manual_undispatch.order_id
    , d.id AS delivery_id
    , DATE(DATETIME(a.created_at, o.timezone)) AS report_date
    , o.timezone
    , hurrier.manual_undispatch.old_status AS log_status
    , TRUE AS rider_notified
  FROM `{{ params.project_id }}.cl.audit_logs` a
  LEFT JOIN `{{ params.project_id }}.cl.orders` o ON a.country_code = o.country_code
    AND a.hurrier.manual_undispatch.order_id = o.order_id
    AND o.created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 6 WEEK)
  LEFT JOIN UNNEST(o.deliveries) d ON hurrier.manual_undispatch.old_rider_id IN (SELECT rider_id FROM UNNEST(d.transitions))
  LEFT JOIN `{{ params.project_id }}.cl._orders_to_zones` oz ON o.country_code = oz.country_code
    AND o.order_id = oz.order_id
    AND oz.created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 6 WEEK)
  WHERE a.created_date >= (SELECT start_date FROM parameters)
    AND action = 'manual_undispatch'
    AND hurrier.manual_undispatch.old_status IN ('accepted', 'courier_notified')
)
---------------- DELIVERIES ----------------
SELECT o.country_code
  , en.entities
  , o.entity.display_name AS entity
  , o.city_id
  , o.zone_id
  , d.rider_id
  , vo.vehicle_profile
  , vo.vehicle_name
  , ba.batch_number
  , o.order_id
  , d.id AS delivery_id
  , COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone)) AS report_date
  , d.rider_dropped_off_at
  , d.rider_picked_up_at
  , d.rider_near_customer_at
  , NULL AS rider_notified_count
  , NULL AS rider_accepted_count
  , NULL AS log_status
  , d.delivery_status
  , cancellation.reason AS cancellation_reason
  , TIMESTAMP_DIFF(d.rider_near_customer_at, d.rider_accepted_at, SECOND) AS actual_delivery_time
  , d.timings.at_vendor_time
  , d.timings.at_customer_time
  , d.timings.rider_reaction_time
  , d.dropoff_distance_manhattan
  , d.pickup_distance_manhattan
  , NULL AS shift_id
  , NULL AS start_at_local
  , NULL AS end_at_local
  , NULL AS hours_worked
  , NULL AS hours_planned
  , NULL AS break_time
  , NULL AS login_difference
  , NULL AS shift_state
  , NULL AS is_unexcused
  , NULL AS peak_time
  , NULL AS is_weekend_shift
  , NULL AS swap_status
  , NULL AS busy_time
  , NULL AS working_time
FROM `{{ params.project_id }}.cl.orders` o
LEFT JOIN UNNEST(o.deliveries) d
LEFT JOIN vehicle_most_orders_ordered vo ON o.country_code = vo.country_code
  AND d.rider_id = vo.rider_id
  AND vo.row_number = 1
  AND COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone)) = vo.report_date
LEFT JOIN batches ba ON o.country_code = ba.country_code
  AND d.rider_id = ba.rider_id
  AND d.created_at >= ba.active_from
  AND d.created_at < ba.active_until
LEFT JOIN entities en ON o.country_code = en.country_code
WHERE o.created_date >= (SELECT start_date FROM parameters)
  AND COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone)) BETWEEN (SELECT start_date FROM parameters) AND (SELECT end_date FROM parameters)

UNION ALL

---------------- ACCEPTANCE RATE ----------------
SELECT o.country_code
  , en.entities
  , o.entity.display_name AS entity
  , o.city_id
  , o.zone_id
  , t.rider_id
  , vo.vehicle_profile
  , vo.vehicle_name
  , ba.batch_number
  , o.order_id
  , d.id AS delivery_id
  , COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone)) AS report_date
  , NULL AS rider_dropped_off_at
  , NULL AS rider_picked_up_at
  , NULL AS rider_near_customer_at
  , IF(t.state = 'courier_notified', 1, 0) rider_notified_count
  , IF(t.state = 'accepted', 1, 0) AS rider_accepted_count
  , NULL AS log_status
  , NULL AS delivery_status
  , NULL AS cancellation_reason
  , NULL AS actual_delivery_time
  , NULL AS at_vendor_time
  , NULL AS at_customer_time
  , NULL AS rider_reaction_time
  , NULL AS dropoff_distance_manhattan
  , NULL AS pickup_distance_manhattan
  , NULL AS shift_id
  , NULL AS start_at_local
  , NULL AS end_at_local
  , NULL AS hours_worked
  , NULL AS hours_planned
  , NULL AS break_time
  , NULL AS login_difference
  , NULL AS shift_state
  , NULL AS is_unexcused
  , NULL AS peak_time
  , NULL AS is_weekend_shift
  , NULL AS swap_status
  , NULL AS busy_time
  , NULL AS working_time
FROM `{{ params.project_id }}.cl.orders` o
LEFT JOIN UNNEST(o.deliveries) d
LEFT JOIN UNNEST(d.transitions) t
LEFT JOIN vehicle_most_orders_ordered vo ON o.country_code = vo.country_code
  AND t.rider_id = vo.rider_id
  AND vo.row_number = 1
  AND COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone)) = vo.report_date
LEFT JOIN batches ba ON o.country_code = ba.country_code
  AND t.rider_id = ba.rider_id
  AND d.created_at >= ba.active_from
  AND d.created_at < ba.active_until
LEFT JOIN entities en ON o.country_code = en.country_code
WHERE o.created_date >= (SELECT start_date FROM parameters)
  AND COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone)) BETWEEN (SELECT start_date FROM parameters) AND (SELECT end_date FROM parameters)

UNION ALL
---------------- SHIFTS ----------------
SELECT s.country_code
  , en.entities
  , IF(ARRAY_LENGTH(co.platforms) = 1, co.platforms[OFFSET(0)].display_name, en.entities) AS entity
  , s.city_id
  , s.zone_id
  , s.rider_id
  , vo.vehicle_profile
  , vo.vehicle_name
  , ba.batch_number
  , NULL AS order_id
  , NULL AS delivery_id
  , DATE(DATETIME(actual_start_at, s.timezone)) AS report_date
  , NULL AS rider_dropped_off_at
  , NULL AS rider_picked_up_at
  , NULL AS rider_near_customer_at
  , NULL AS rider_notified_count
  , NULL AS rider_accepted_count
  , NULL AS log_status
  , NULL AS delivery_status
  , NULL AS cancellation_reason
  , NULL AS actual_delivery_time
  , NULL AS at_vendor_time
  , NULL AS at_customer_time
  , NULL AS rider_reaction_time
  , NULL AS dropoff_distance_manhattan
  , NULL AS pickup_distance_manhattan
  , s.shift_id
  , TIMESTAMP(DATETIME(s.actual_start_at, s.timezone)) AS start_at_local
  , TIMESTAMP(DATETIME(s.actual_end_at, s.timezone)) AS end_at_local
  , ROUND(actual_working_time / 3600, 2) AS hours_worked
  , ROUND(TIMESTAMP_DIFF(s.shift_end_at, s.shift_start_at, SECOND) / 3600, 2) AS hours_planned
  , ROUND(actual_break_time /60, 2) AS break_time
  , ROUND(login_difference / 60, 2) AS login_difference
  , s.shift_state
  , NULL AS is_unexcused
  , tp.peak_time
  , IF(FORMAT_DATE("%u", DATE(DATETIME(s.actual_start_at, s.timezone))) IN ('5', '6', '7') AND actual_working_time > 0, TRUE, FALSE) AS is_weekend_shift
  , NULL AS swap_status
  , NULL AS busy_time
  , NULL AS working_time
FROM `{{ params.project_id }}.cl.shifts` s
LEFT JOIN batches ba ON s.country_code = ba.country_code
  AND s.rider_id = ba.rider_id
  AND actual_start_at >= ba.active_from
  AND actual_start_at < ba.active_until
LEFT JOIN vehicle_most_orders_ordered vo ON s.country_code = vo.country_code
  AND s.rider_id = vo.rider_id
  AND DATE(DATETIME(s.actual_start_at, s.timezone)) = vo.report_date
  AND vo.row_number = 1
LEFT JOIN temps_shifts_final tp ON s.country_code = tp.country_code
  AND s.shift_id = tp.shift_id
  AND DATE(DATETIME(actual_start_at, s.timezone)) = tp.report_date
LEFT JOIN countries co ON s.country_code = co.country_code
  AND s.city_id = co.city_id
  AND s.zone_id = co.zone_id
LEFT JOIN entities en ON s.country_code = en.country_code
WHERE s.shift_state IN ('EVALUATED')
  AND s.created_date >= (SELECT start_date FROM parameters)
  AND DATE(DATETIME(actual_start_at, s.timezone)) BETWEEN (SELECT start_date FROM parameters) AND (SELECT end_date FROM parameters)

UNION ALL
---------------- UNDISPATCHED ORDERS ----------------
SELECT l.country_code
  , en.entities
  , l.entity
  , l.city_id
  , l.zone_id
  , l.rider_id
  , vo.vehicle_profile
  , vo.vehicle_name
  , NULL AS batch_number
  , l.order_id
  , l.delivery_id
  , l.report_date
  , NULL AS rider_dropped_off_at
  , NULL AS rider_picked_up_at
  , NULL AS rider_near_customer_at
  , NULL AS rider_notified_count
  , NULL AS rider_accepted_count
  , l.log_status  ------ applies to the rider whom the order was undispatched from
  , NULL AS delivery_status
  , NULL AS cancellation_reason
  , NULL AS actual_delivery_time
  , NULL AS at_vendor_time
  , NULL AS at_customer_time
  , NULL AS rider_reaction_time
  , NULL AS dropoff_distance_manhattan
  , NULL AS pickup_distance_manhattan
  , NULL AS shift_id
  , NULL AS start_at_local
  , NULL AS end_at_local
  , NULL AS hours_worked
  , NULL AS hours_planned
  , NULL AS break_time
  , NULL AS login_difference
  , NULL AS shift_state
  , NULL AS is_unexcused
  , NULL AS peak_time
  , NULL AS is_weekend_shift
  , NULL AS swap_status
  , NULL AS busy_time
  , NULL AS working_time
FROM logs l
LEFT JOIN vehicle_most_orders_ordered vo ON l.country_code = vo.country_code
  AND l.rider_id = vo.rider_id
  AND vo.row_number = 1
  AND l.report_date = vo.report_date
LEFT JOIN entities en ON l.country_code = en.country_code

UNION ALL
---------------- NO SHOWS ----------------
SELECT s.country_code
  , en.entities
  , IF(ARRAY_LENGTH(co.platforms) = 1, co.platforms[OFFSET(0)].display_name, en.entities) AS entity
  , s.city_id
  , s.zone_id
  , s.rider_id
  , NULL AS vehicle_profile
  , NULL AS vehicle_name
  , ba.batch_number
  , NULL AS order_id
  , NULL AS delivery_id
  , DATE(DATETIME(s.shift_start_at, s.timezone)) AS report_date
  , NULL AS rider_dropped_off_at
  , NULL AS rider_picked_up_at
  , NULL AS rider_near_customer_at
  , NULL AS rider_notified_count
  , NULL AS rider_accepted_count
  , NULL AS log_status
  , NULL AS delivery_status
  , NULL AS cancellation_reason
  , NULL AS actual_delivery_time
  , NULL AS at_vendor_time
  , NULL AS at_customer_time
  , NULL AS rider_reaction_time
  , NULL AS dropoff_distance_manhattan
  , NULL AS pickup_distance_manhattan
  , s.shift_id
  , NULL AS start_at_local
  , NULL AS end_at_local
  , NULL AS hours_worked
  , NULL AS hours_planned
  , NULL AS break_time
  , NULL AS login_difference
  , s.shift_state
  , ARRAY_LENGTH(absences) = 0 AS is_unexcused
  , NULL AS peak_time
  , NULL AS is_weekend_shift
  , sw.status AS swap_status
  , NULL AS busy_time
  , NULL AS working_time
FROM `{{ params.project_id }}.cl.shifts` s
LEFT JOIN batches ba ON s.country_code = ba.country_code
  AND s.rider_id = ba.rider_id
  AND s.shift_start_at >= ba.active_from
  AND s.shift_start_at < ba.active_until
LEFT JOIN countries co ON s.country_code = co.country_code
LEFT JOIN entities en ON s.country_code = en.country_code
LEFT JOIN swaps sw ON s.country_code = sw.country_code  ------- temporarily remove swaps
  AND s.shift_id = sw.shift_id
  AND sw.row_number = 1
WHERE s.shift_state IN ('EVALUATED', 'NO_SHOW', 'NO_SHOW_EXCUSED')
  AND s.created_date >= (SELECT start_date FROM parameters)

UNION ALL
---------------- IDLE TIME ----------------
SELECT b.country_code
  , e.entities
  , IF(ARRAY_LENGTH(co.platforms) = 1, co.platforms[OFFSET(0)].display_name, e.entities) AS entity
  , b.city_id
  , b.zone_id
  , b.rider_id
  , vo.vehicle_profile
  , vo.vehicle_name
  , ba.batch_number
  , NULL AS order_id
  , NULL AS delivery_id
  , b.working_day AS report_date
  , NULL AS rider_dropped_off_at
  , NULL AS rider_picked_up_at
  , NULL AS rider_near_customer_at
  , NULL AS rider_notified_count
  , NULL AS rider_accepted_count
  , NULL AS log_status
  , NULL AS delivery_status
  , NULL AS cancellation_reason
  , NULL AS actual_delivery_time
  , NULL AS at_vendor_time
  , NULL AS at_customer_time
  , NULL AS rider_reaction_time
  , NULL AS dropoff_distance_manhattan
  , NULL AS pickup_distance_manhattan
  , NULL AS shift_id
  , NULL AS start_at_local
  , NULL AS end_at_local
  , NULL AS hours_worked
  , NULL AS hours_planned
  , NULL AS break_time
  , NULL AS login_difference
  , NULL AS shift_state
  , NULL AS is_unexcused
  , NULL AS peak_time
  , NULL AS is_weekend_shift
  , NULL AS swap_status
  , IF(b.state = 'busy', duration, 0) AS busy_time
  , (IF(b.state = 'working', duration, 0)) AS working_time
FROM `{{ params.project_id }}.cl._rider_working_time` b
LEFT JOIN batches ba ON b.country_code = ba.country_code
  AND b.rider_id = ba.rider_id
  AND TIMESTAMP(b.started_at_local, b.timezone) >= ba.active_from  ----------- Reverts local time to UTC
  AND TIMESTAMP(b.started_at_local, b.timezone) < ba.active_until
LEFT JOIN vehicle_most_orders_ordered vo ON b.country_code = vo.country_code
  AND b.rider_id = vo.rider_id
  AND vo.row_number = 1
  AND b.working_day = vo.report_date
LEFT JOIN countries co ON b.country_code = co.country_code
  AND b.city_id = co.city_id
  AND b.zone_id = co.zone_id
LEFT JOIN entities e ON b.country_code = e.country_code
WHERE working_day >= (SELECT start_date FROM parameters)
;
