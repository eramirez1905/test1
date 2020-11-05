CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._rider_kpi_source`
PARTITION BY created_date_local AS
WITH countries AS (
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
), countries_timezone AS (
  SELECT c.country_code
    -- taking one timezone per country to use in the final table
    , (SELECT timezone FROM UNNEST (cities) ci ORDER BY 1 LIMIT 1) AS timezone
  FROM `{{ params.project_id }}.cl.countries` c
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
        WHEN LOWER(weekday) = 'monday'    THEN 1
        WHEN LOWER(weekday) = 'tuesday'   THEN 2
        WHEN LOWER(weekday) = 'wednesday' THEN 3
        WHEN LOWER(weekday) = 'thursday'  THEN 4
        WHEN LOWER(weekday) = 'friday'    THEN 5
        WHEN LOWER(weekday) = 'saturday'  THEN 6
        WHEN LOWER(weekday) = 'sunday'    THEN 7
     END AS weekday
    , start_time AS start_peak
    , end_time AS end_peak
    , period_name AS time_period
    , ROW_NUMBER() OVER (PARTITION BY country_code, zone_id, lower(weekday),
      start_time ORDER BY created_at DESC, u.updated_at DESC) AS ranking
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
    , COALESCE(DATE(actual_start_at, COALESCE(s.timezone, ct.timezone)),
      DATE(shift_start_at, COALESCE(s.timezone, ct.timezone))) AS created_date_local
    , tp.start_peak
    , tp.end_peak
    , LEAST(CAST(end_peak AS TIME), IF(DATE(s.actual_end_at,
      COALESCE(s.timezone, ct.timezone)) != DATE(actual_start_at, COALESCE(s.timezone, ct.timezone)),
      '23:59:59', TIME(DATETIME(actual_end_at, COALESCE(s.timezone, ct.timezone))))) AS least
    , GREATEST(CAST(start_peak AS TIME), CAST(TIMESTAMP(DATETIME(s.actual_start_at,
      COALESCE(s.timezone, ct.timezone))) AS TIME)) AS greatest
  FROM `{{ params.project_id }}.cl.shifts` s
  LEFT JOIN countries_timezone ct ON s.country_code = ct.country_code
  LEFT JOIN temp_periods tp ON s.country_code = tp.country_code
    AND s.city_id = tp.city_id
    AND s.zone_id = tp.zone_id
    AND FORMAT_DATE("%u", CAST(COALESCE(DATETIME(actual_start_at, COALESCE(s.timezone, ct.timezone)),
      DATETIME(shift_start_at, COALESCE(s.timezone, ct.timezone))) AS DATE)) = CAST(tp.weekday AS STRING)
    AND tp.time_period IN ('lunch', 'dinner', 'Lunch', 'Dinner')
    AND TIME(COALESCE(DATETIME(actual_start_at, COALESCE(s.timezone, ct.timezone)),
      DATETIME(shift_start_at, COALESCE(s.timezone, ct.timezone)))) <= tp.end_peak
    AND tp.start_peak <= IF(COALESCE(DATE(actual_end_at, COALESCE(s.timezone, ct.timezone)),
      DATE(shift_end_at, COALESCE(s.timezone, ct.timezone))) !=
      COALESCE(DATE(actual_start_at,COALESCE(s.timezone, ct.timezone)),
      DATE(shift_start_at, COALESCE(s.timezone, ct.timezone))), '23:59:59',
      TIME(COALESCE(DATETIME(actual_end_at, COALESCE(s.timezone, ct.timezone)),
      DATETIME(shift_end_at, COALESCE(s.timezone, ct.timezone)))))
  WHERE s.shift_state = 'EVALUATED'
), temps_shifts_final AS (
  SELECT country_code
    , shift_id
    , created_date_local
    , COALESCE(
      IF(SUM(TIME_DIFF(least, greatest, SECOND) / 60) < 0,
        SUM(TIME_DIFF(end_peak, start_peak, SECOND)),
        SUM(TIME_DIFF(least, greatest, SECOND))
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
    , DATE(s.shift_start_at, COALESCE(s.timezone, ct.timezone)) AS start_date_local
    , s.rider_id
    -- take last transition in swap history for each shift
    , ROW_NUMBER() OVER (PARTITION BY sw.country_code, sw.shift_id, s.rider_id ORDER BY sw.created_at DESC) AS row_number
    , s.shift_state
    , sw.status
    , sw.shift_id
  FROM shift_swap_requests sw
  LEFT JOIN `{{ params.project_id }}.cl.shifts` s ON sw.country_code = s.country_code
    AND sw.shift_id = s.shift_id
  LEFT JOIN countries_timezone ct ON s.country_code = ct.country_code
), vehicle_dataset AS (
  SELECT o.country_code
    , rider_id
    , id
    , COALESCE(DATE(d.rider_dropped_off_at, COALESCE(o.timezone, ct.timezone)),
      DATE(d.created_at, COALESCE(o.timezone, ct.timezone))) AS created_date_local
    , vehicle.profile AS vehicle_profile
    , vehicle.name AS vehicle_name
  FROM `{{ params.project_id }}.cl.orders` o
  LEFT JOIN UNNEST (deliveries) d
  LEFT JOIN countries_timezone ct ON o.country_code = ct.country_code
  WHERE d.delivery_status = 'completed'
  ORDER BY country_code, rider_id, created_date, rider_dropped_off_at
), vehicle_most_orders_ordered AS (
  SELECT country_code
    , rider_id
    , created_date_local
    , vehicle_profile
    , vehicle_name
    , ROW_NUMBER() OVER (PARTITION BY country_code, rider_id, created_date_local
      ORDER BY COUNT(DISTINCT id) DESC) AS row_number
    , COUNT(id) AS deliveries
  FROM vehicle_dataset
  GROUP BY 1, 2, 3, 4, 5
), logs AS (
  SELECT a.country_code
    , o.city_id
    , o.zone_id
    , hurrier.manual_undispatch.old_rider_id AS rider_id
    , hurrier.manual_undispatch.order_id
    , d.id AS delivery_id
    , DATE(a.created_at, COALESCE(a.timezone, ct.timezone)) AS created_date_local
    , o.timezone
    , hurrier.manual_undispatch.old_status AS log_status
    , TRUE AS rider_notified
  FROM `{{ params.project_id }}.cl.audit_logs` a
  LEFT JOIN `{{ params.project_id }}.cl.orders` o ON a.country_code = o.country_code
    AND a.hurrier.manual_undispatch.order_id = o.order_id
  LEFT JOIN UNNEST(o.deliveries) d ON hurrier.manual_undispatch.old_rider_id
    IN (SELECT rider_id FROM UNNEST(d.transitions))
    AND action = 'manual_undispatch'
    AND hurrier.manual_undispatch.old_status IN ('accepted', 'courier_notified')
  LEFT JOIN countries_timezone ct ON o.country_code = ct.country_code
), last_delivery_base AS (
  SELECT o.country_code
  , o.city_id
  , o.zone_id
  , d.rider_id
  , o.order_id
  , d.id AS delivery_id
  , COALESCE(DATETIME(d.rider_dropped_off_at, COALESCE(o.timezone, ct.timezone)),
    DATE(d.created_at, COALESCE(o.timezone, ct.timezone))) AS created_date_local
  , d.timings.at_customer_time
  FROM `{{ params.project_id }}.cl.orders` o
  LEFT JOIN UNNEST(o.deliveries) d
  LEFT JOIN countries_timezone ct ON o.country_code = ct.country_code
  WHERE d.delivery_status = 'completed'
), last_delivery AS (
  SELECT country_code
  , city_id
  , zone_id
  , rider_id
  , order_id
  , delivery_id
  , created_date_local
  , IF(at_customer_time/60 > 10, TRUE, FALSE) AS at_customer_time_over_10
  , ROW_NUMBER() OVER(PARTITION BY rider_id, created_date_local ORDER BY created_date_local DESC)
    AS delivery_rank
  FROM last_delivery_base
), last_delivery_final AS (
  SELECT country_code
  , city_id
  , zone_id
  , rider_id
  , order_id
  , delivery_id
  , created_date_local
  , IF (at_customer_time_over_10 IS TRUE AND delivery_rank = 1, TRUE, FALSE) AS last_delivery_over_10
  FROM last_delivery
)

---------------- DELIVERIES ----------------
SELECT o.country_code
  , o.city_id
  , o.zone_id
  , d.rider_id
  , vo.vehicle_profile
  , vo.vehicle_name
  , ba.batch_number
  , o.order_id
  , d.id AS delivery_id
  , COALESCE(DATE(d.rider_dropped_off_at, COALESCE(o.timezone, ct.timezone)),
    DATE(d.created_at, COALESCE(o.timezone, ct.timezone)),
    DATE(o.created_at, COALESCE(o.timezone, ct.timezone))) AS created_date_local
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
  , d.timings.to_vendor_time
  , d.timings.to_customer_time
  , d.dropoff_distance_manhattan
  , d.pickup_distance_manhattan
  , l.last_delivery_over_10
  , NULL AS shift_id
  , NULL AS start_at_local
  , NULL AS end_at_local
  , NULL AS working_time
  , NULL AS planned_working_time
  , NULL AS break_time
  , NULL AS login_difference
  , NULL AS shift_state
  , NULL AS is_unexcused
  , NULL AS peak_time
  , NULL AS is_weekend_shift
  , NULL AS swap_status
  , NULL AS transition_busy_time
  , NULL AS transition_working_time
FROM `{{ params.project_id }}.cl.orders` o
LEFT JOIN UNNEST(o.deliveries) d
LEFT JOIN countries_timezone ct ON o.country_code = ct.country_code
LEFT JOIN vehicle_most_orders_ordered vo ON o.country_code = vo.country_code
  AND d.rider_id = vo.rider_id
  AND vo.row_number = 1
  AND COALESCE(DATE(d.rider_dropped_off_at, COALESCE(o.timezone, ct.timezone)),
    DATE(d.created_at, COALESCE(o.timezone, ct.timezone)),
    DATE(o.created_at, COALESCE(o.timezone, ct.timezone))) = vo.created_date_local
LEFT JOIN batches ba ON o.country_code = ba.country_code
  AND d.rider_id = ba.rider_id
  AND d.created_at >= ba.active_from
  AND d.created_at < ba.active_until
LEFT JOIN last_delivery_final l ON o.country_code = l.country_code
  AND d.rider_id = l.rider_id
  AND o.order_id = l.order_id
  AND d.id = l.delivery_id
  AND COALESCE(DATE(d.rider_dropped_off_at, COALESCE(o.timezone, ct.timezone)),
    DATE(d.created_at, COALESCE(o.timezone, ct.timezone)),
    DATE(o.created_at, COALESCE(o.timezone, ct.timezone))) = DATE(l.created_date_local)

UNION ALL
---------------- ACCEPTANCE RATE ----------------
SELECT o.country_code
  , o.city_id
  , o.zone_id
  , t.rider_id
  , vo.vehicle_profile
  , vo.vehicle_name
  , ba.batch_number
  , o.order_id
  , d.id AS delivery_id
  , COALESCE(DATE(d.rider_dropped_off_at, COALESCE(o.timezone, ct.timezone)),
    DATE(d.created_at, COALESCE(o.timezone, ct.timezone)),
    DATE(o.created_at, COALESCE(o.timezone, ct.timezone))) AS created_date_local
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
  , NULL AS to_vendor_time
  , NULL AS to_customer_time
  , NULL AS dropoff_distance_manhattan
  , NULL AS pickup_distance_manhattan
  , NULL AS last_delivery_over_10
  , NULL AS shift_id
  , NULL AS start_at_local
  , NULL AS end_at_local
  , NULL AS working_time
  , NULL AS planned_working_time
  , NULL AS break_time
  , NULL AS login_difference
  , NULL AS shift_state
  , NULL AS is_unexcused
  , NULL AS peak_time
  , NULL AS is_weekend_shift
  , NULL AS swap_status
  , NULL AS transition_busy_time
  , NULL AS transition_working_time
FROM `{{ params.project_id }}.cl.orders` o
LEFT JOIN UNNEST(o.deliveries) d
LEFT JOIN UNNEST(d.transitions) t
LEFT JOIN countries_timezone ct ON o.country_code = ct.country_code
LEFT JOIN vehicle_most_orders_ordered vo ON o.country_code = vo.country_code
  AND t.rider_id = vo.rider_id
  AND vo.row_number = 1
  AND COALESCE(DATE(d.rider_dropped_off_at, COALESCE(o.timezone, ct.timezone)),
    DATE(d.created_at, COALESCE(o.timezone, ct.timezone)),
    DATE(o.created_at, COALESCE(o.timezone, ct.timezone))) = vo.created_date_local
LEFT JOIN batches ba ON o.country_code = ba.country_code
  AND t.rider_id = ba.rider_id
  AND d.created_at >= ba.active_from
  AND d.created_at < ba.active_until

UNION ALL
---------------- SHIFTS ----------------
SELECT s.country_code
  , s.city_id
  , s.zone_id
  , s.rider_id
  , vo.vehicle_profile
  , vo.vehicle_name
  , ba.batch_number
  , NULL AS order_id
  , NULL AS delivery_id
  , COALESCE(DATE(s.actual_start_at, COALESCE(s.timezone, ct.timezone)),
    DATE(s.shift_start_at,COALESCE(s.timezone, ct.timezone))) AS created_date_local
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
  , NULL AS to_vendor_time
  , NULL AS to_customer_time
  , NULL AS dropoff_distance_manhattan
  , NULL AS pickup_distance_manhattan
  , NULL AS last_delivery_over_10
  , s.shift_id
  , TIMESTAMP(DATETIME(s.actual_start_at, COALESCE(s.timezone, ct.timezone))) AS start_at_local
  , TIMESTAMP(DATETIME(s.actual_end_at, COALESCE(s.timezone, ct.timezone))) AS end_at_local
  , actual_working_time AS working_time
  , TIMESTAMP_DIFF(s.shift_end_at, s.shift_start_at, SECOND) AS planned_working_time
  , actual_break_time AS break_time
  , login_difference AS login_difference
  , s.shift_state
  , NULL AS is_unexcused
  , tp.peak_time
  , IF(FORMAT_DATE("%u", DATE(s.actual_start_at, COALESCE(s.timezone, ct.timezone)))
    IN ('5', '6', '7') AND actual_working_time > 0, TRUE, FALSE) AS is_weekend_shift
  , NULL AS swap_status
  , NULL AS transition_busy_time
  , NULL AS transition_working_time
FROM `{{ params.project_id }}.cl.shifts` s
LEFT JOIN countries_timezone ct ON s.country_code = ct.country_code
LEFT JOIN batches ba ON s.country_code = ba.country_code
  AND s.rider_id = ba.rider_id
  AND COALESCE(TIMESTAMP(DATETIME(s.actual_start_at), COALESCE(s.timezone, ct.timezone)),
    TIMESTAMP(DATETIME(s.shift_start_at), COALESCE(s.timezone, ct.timezone))) >= ba.active_from
  AND COALESCE(TIMESTAMP(DATETIME(s.actual_start_at), COALESCE(s.timezone, ct.timezone)),
    TIMESTAMP(DATETIME(s.shift_start_at), COALESCE(s.timezone, ct.timezone))) < ba.active_until
LEFT JOIN vehicle_most_orders_ordered vo ON s.country_code = vo.country_code
  AND s.rider_id = vo.rider_id
  AND COALESCE(DATE(s.actual_start_at, COALESCE(s.timezone, ct.timezone)),
    DATE(s.shift_start_at, COALESCE(s.timezone, ct.timezone))) = vo.created_date_local
  AND vo.row_number = 1
LEFT JOIN temps_shifts_final tp ON s.country_code = tp.country_code
  AND s.shift_id = tp.shift_id
  AND COALESCE(DATE(s.actual_start_at, COALESCE(s.timezone, ct.timezone)),
    DATE(s.shift_start_at,COALESCE(s.timezone, ct.timezone))) = tp.created_date_local
LEFT JOIN countries co ON s.country_code = co.country_code
  AND s.city_id = co.city_id
  AND s.zone_id = co.zone_id
WHERE s.shift_state IN ('EVALUATED')

UNION ALL
---------------- UNDISPATCHED ORDERS ----------------
SELECT l.country_code
  , l.city_id
  , l.zone_id
  , l.rider_id
  , vo.vehicle_profile
  , vo.vehicle_name
  , ba.batch_number
  , l.order_id
  , l.delivery_id
  , l.created_date_local
  , NULL AS rider_dropped_off_at
  , NULL AS rider_picked_up_at
  , NULL AS rider_near_customer_at
  , NULL AS rider_notified_count
  , NULL AS rider_accepted_count
  -- applies to the rider whom the order was undispatched from
  , l.log_status
  , NULL AS delivery_status
  , NULL AS cancellation_reason
  , NULL AS actual_delivery_time
  , NULL AS at_vendor_time
  , NULL AS at_customer_time
  , NULL AS rider_reaction_time
  , NULL AS to_vendor_time
  , NULL AS to_customer_time
  , NULL AS dropoff_distance_manhattan
  , NULL AS pickup_distance_manhattan
  , NULL AS last_delivery_over_10
  , NULL AS shift_id
  , NULL AS start_at_local
  , NULL AS end_at_local
  , NULL AS working_time
  , NULL AS planned_working_time
  , NULL AS break_time
  , NULL AS login_difference
  , NULL AS shift_state
  , NULL AS is_unexcused
  , NULL AS peak_time
  , NULL AS is_weekend_shift
  , NULL AS swap_status
  , NULL AS transition_busy_time
  , NULL AS transition_working_time
FROM logs l
LEFT JOIN batches ba ON l.country_code = ba.country_code
  AND l.rider_id = ba.rider_id
  -- Reverts local time to UTC
  AND TIMESTAMP(l.created_date_local, 'UTC') >= ba.active_from
  AND TIMESTAMP(l.created_date_local, 'UTC') < ba.active_until
LEFT JOIN vehicle_most_orders_ordered vo ON l.country_code = vo.country_code
  AND l.rider_id = vo.rider_id
  AND vo.row_number = 1
  AND l.created_date_local = vo.created_date_local

UNION ALL
---------------- NO SHOWS ----------------
SELECT s.country_code
  , s.city_id
  , s.zone_id
  , s.rider_id
  , vo.vehicle_profile
  , vo.vehicle_name
  , ba.batch_number
  , NULL AS order_id
  , NULL AS delivery_id
  , DATE(s.shift_start_at, COALESCE(s.timezone, ct.timezone)) AS created_date_local
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
  , NULL AS to_vendor_time
  , NULL AS to_customer_time
  , NULL AS dropoff_distance_manhattan
  , NULL AS pickup_distance_manhattan
  , NULL AS last_delivery_over_10
  , s.shift_id
  , NULL AS start_at_local
  , NULL AS end_at_local
  , NULL AS working_time
  , NULL AS planned_working_time
  , NULL AS break_time
  , NULL AS login_difference
  , s.shift_state
  , ARRAY_LENGTH(absences) = 0 AS is_unexcused
  , NULL AS peak_time
  , NULL AS is_weekend_shift
  , sw.status AS swap_status
  , NULL AS transition_busy_time
  , NULL AS transition_working_time
FROM `{{ params.project_id }}.cl.shifts` s
LEFT JOIN countries_timezone ct ON s.country_code = ct.country_code
LEFT JOIN batches ba ON s.country_code = ba.country_code
  AND s.rider_id = ba.rider_id
  AND TIMESTAMP(DATETIME(s.shift_start_at), COALESCE(s.timezone, ct.timezone)) >= ba.active_from
  AND TIMESTAMP(DATETIME(s.shift_start_at), COALESCE(s.timezone, ct.timezone)) < ba.active_until
LEFT JOIN countries co ON s.country_code = co.country_code
-- temporarily remove swaps
LEFT JOIN swaps sw ON s.country_code = sw.country_code
  AND s.shift_id = sw.shift_id
  AND sw.row_number = 1
LEFT JOIN vehicle_most_orders_ordered vo ON s.country_code = vo.country_code
  AND s.rider_id = vo.rider_id
  AND vo.row_number = 1
  AND DATE(s.shift_start_at, COALESCE(s.timezone, ct.timezone)) = vo.created_date_local
WHERE s.shift_state IN ('EVALUATED', 'NO_SHOW', 'NO_SHOW_EXCUSED')

UNION ALL
---------------- IDLE TIME ----------------
SELECT b.country_code
  , b.city_id
  , b.zone_id
  , b.rider_id
  , vo.vehicle_profile
  , vo.vehicle_name
  , ba.batch_number
  , NULL AS order_id
  , NULL AS delivery_id
  , b.working_day AS created_date_local
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
  , NULL AS to_vendor_time
  , NULL AS to_customer_time
  , NULL AS dropoff_distance_manhattan
  , NULL AS pickup_distance_manhattan
  , NULL AS last_delivery_over_10
  , NULL AS shift_id
  , NULL AS start_at_local
  , NULL AS end_at_local
  , NULL AS working_time
  , NULL AS planned_working_time
  , NULL AS break_time
  , NULL AS login_difference
  , NULL AS shift_state
  , NULL AS is_unexcused
  , NULL AS peak_time
  , NULL AS is_weekend_shift
  , NULL AS swap_status
  -- Transform minutes to seconds
  , IF(b.state = 'busy', duration, 0) * 60 AS transition_busy_time
  , IF(b.state = 'working', duration, 0) * 60 AS transition_working_time
FROM `{{ params.project_id }}.cl._rider_working_time` b
LEFT JOIN batches ba ON b.country_code = ba.country_code
  AND b.rider_id = ba.rider_id
  -- Reverts local time to UTC
  AND TIMESTAMP(b.started_at_local, b.timezone) >= ba.active_from
  AND TIMESTAMP(b.started_at_local, b.timezone) < ba.active_until
LEFT JOIN vehicle_most_orders_ordered vo ON b.country_code = vo.country_code
  AND b.rider_id = vo.rider_id
  AND vo.row_number = 1
  AND b.working_day = vo.created_date_local
LEFT JOIN countries co ON b.country_code = co.country_code
  AND b.city_id = co.city_id
  AND b.zone_id = co.zone_id

