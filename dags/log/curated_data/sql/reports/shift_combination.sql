CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.shift_combination`
PARTITION BY report_date_local AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 6 WEEK) AS start_date
), countries AS (
  SELECT country_code
    , country_name
    , ci.id AS city_id
    , ci.name AS city_name
    , ci.timezone AS timezone
    , zo.id AS zone_id
    , zo.name AS zone_name
    , sp.id AS starting_point_id
    , sp.name AS starting_point_name
  FROM `{{ params.project_id }}.cl.countries` co
  LEFT JOIN UNNEST(co.cities) ci
  LEFT JOIN UNNEST(ci.zones) zo
  LEFT JOIN UNNEST (zo.starting_points) sp
), batches AS (
  SELECT country_code
    , rider_id
    , b.number AS batch_number
    , b.active_from
    , b.active_until
  FROM `{{ params.project_id }}.cl.riders` r
  LEFT JOIN UNNEST(batches) b
), orders AS (
  SELECT o.country_code
    , d.rider_dropped_off_at
    , o.platform_order_code
    , s.shift_id
    , o.order_id
    , o.created_date
    , TIMESTAMP_ADD(s.actual_end_at, INTERVAL 1 MINUTE) AS actual_shift_end_buffer
    , TIMESTAMP_ADD(d.rider_dropped_off_at, INTERVAL 1 MINUTE) AS delivered_dropped_off_at_buffer
  FROM `{{ params.project_id }}.cl.orders` o
  LEFT JOIN UNNEST(deliveries) d ON d.is_primary
  LEFT JOIN  `{{ params.project_id }}.cl.shifts` s ON o.country_code = s.country_code
    AND d.rider_id = s.rider_id
    AND TIMESTAMP_ADD(d.rider_dropped_off_at, INTERVAL 1 MINUTE) BETWEEN TIMESTAMP_SUB(s.actual_start_at, INTERVAL 1 MINUTE) AND TIMESTAMP_ADD(s.actual_end_at, INTERVAL 1 MINUTE)
  WHERE d.delivery_status = 'completed'
    AND o.created_date >= (SELECT start_date FROM parameters)
), orders_cleaned AS (
  SELECT country_code
    , rider_dropped_off_at
    , platform_order_code
    , shift_id
    , order_id
    , created_date
    , ROW_NUMBER() OVER (PARTITION BY country_code, order_id, created_date, rider_dropped_off_at ORDER BY TIMESTAMP_DIFF(actual_shift_end_buffer, delivered_dropped_off_at_buffer, SECOND) DESC) AS overlap_rank
  FROM orders
), order_final AS (
  SELECT country_code
    , shift_id
    , COUNT(order_id) AS orders_completed
  FROM orders_cleaned
  WHERE overlap_rank = 1
    AND country_code NOT LIKE '%dp%'
  GROUP BY 1, 2
), shifts AS(
  SELECT s.country_code
    , s.timezone
    , DATE(s.shift_start_at, s.timezone) AS report_date_local
    , TIME(TIMESTAMP_TRUNC(s.shift_start_at, SECOND), s.timezone) AS report_time_local
    , s.shift_id
    , s.rider_id
    , s.starting_point_id
    , s.shift_start_at
    , s.shift_end_at
    , s.planned_shift_duration
    , LAG(s.shift_end_at) OVER (PARTITION BY s.rider_id ORDER BY s.shift_start_at ASC) AS before_shift_end_at
    , LEAD(s.shift_start_at) OVER (PARTITION BY s.rider_id ORDER BY s.shift_start_at ASC) AS next_shift_start_at
    , s.actual_start_at
    , s.actual_end_at
    , s.actual_working_time
    , LAG(s.actual_end_at) OVER (PARTITION BY s.rider_id ORDER BY s.actual_end_at ASC) AS before_actual_end_at
    , LEAD(s.actual_start_at) OVER (PARTITION BY s.rider_id ORDER BY s.actual_start_at ASC) AS next_actual_start_at
    , s.shift_state
    , s.absences
    , deliveries.accepted AS accepted_deliveries
    , deliveries.notified AS notified_deliveries
  FROM `{{ params.project_id }}.cl.shifts` s
  WHERE s.created_date >= (SELECT start_date FROM parameters)
    AND s.shift_state <> 'CANCELLED'
), shift_agg AS (
  SELECT s.country_code
    , DATE(s.shift_start_at, s.timezone) AS report_date_local
    , CASE
      WHEN s.report_time_local BETWEEN '00:00:00' AND '03:00:00'
        THEN 'Night / 00:00 to 03:00'
      WHEN s.report_time_local BETWEEN '03:00:01' AND '06:00:00'
        THEN 'Late Night / 03:00 to 06:00'
      WHEN s.report_time_local BETWEEN '06:00:01' AND '09:00:00'
        THEN ' Morning / 06:00 to 09:00'
      WHEN s.report_time_local BETWEEN '09:00:01' AND '12:00:00'
        THEN 'Late Morning / 09:00 to 12:00'
      WHEN s.report_time_local BETWEEN '12:00:01' AND '15:00:00'
        THEN 'Afternoon / 12:00 - 15:00'
      WHEN s.report_time_local BETWEEN '15:00:01' AND '18:00:00'
        THEN 'Late Afternoon / 15:00 - 18:00'
      WHEN s.report_time_local BETWEEN '18:00:01' AND '21:00:00'
        THEN 'Evening / 18:00 - 21:00'
      WHEN s.report_time_local BETWEEN '21:00:01' AND '23:59:59'
        THEN 'Late Evening / 21:00 - 24:00'
    END AS shift_start_type
    , s.shift_id
    , s.starting_point_id
    , o.orders_completed
    , s.rider_id
    , b.batch_number
    , s.accepted_deliveries
    , s.notified_deliveries
    , s.shift_start_at
    , s.shift_end_at
    , s.planned_shift_duration
    , s.actual_start_at
    , s.actual_end_at
    , s.actual_working_time
    , s.shift_state
    , s.absences
    , CASE
      WHEN TIMESTAMP_DIFF(s.actual_start_at, s.shift_start_at, MINUTE) > 0
        THEN 'Rider Late'
      WHEN TIMESTAMP_DIFF(s.actual_start_at, s.shift_start_at, MINUTE) < 0
        THEN 'Rider Early'
      WHEN TIMESTAMP_DIFF(s.actual_start_at, s.shift_start_at, MINUTE) = 0
        THEN 'Rider On Time'
      END AS rider_login
    , IF(TIMESTAMP_DIFF(s.shift_start_at, s.before_shift_end_at, MINUTE) <= 30 OR TIMESTAMP_DIFF(s.next_shift_start_at, s.shift_end_at, MINUTE) <= 30, 'Combined', 'Standalone') AS shift_marker_plan
    , IF(TIMESTAMP_DIFF(s.actual_start_at, s.before_actual_end_at, MINUTE) <= 30 OR TIMESTAMP_DIFF(s.next_actual_start_at, s.actual_end_at, MINUTE) <= 30, 'Combined', 'Standalone') AS shift_marker_actual
  FROM shifts s
  LEFT JOIN batches b ON b.country_code = s.country_code
    AND b.rider_id = s.rider_id
    AND s.shift_start_at BETWEEN b.active_from AND b.active_until
  LEFT JOIN order_final o ON o.country_code = s.country_code
    AND o.shift_id = s.shift_id
  -- removing darkstore pickers to not impact utr as they do not have orders, only working hours
  WHERE s.country_code NOT LIKE '%dp%'
    -- shifts with no timezones cannot contribute for UTR
    AND s.timezone IS NOT NULL
)
SELECT s.country_code
  , c.country_name
  , c.city_id
  , c.city_name
  , c.zone_id
  , c.zone_name
  , c.starting_point_id
  , c.starting_point_name
  , s.report_date_local
  , s.shift_start_type
  , s.batch_number
  , s.shift_marker_plan
  , s.shift_marker_actual
  , SUM(s.orders_completed) AS completed_orders
  , SUM(s.notified_deliveries) AS notified_deliveries
  , SUM(s.accepted_deliveries) AS accepted_deliveries
  , COUNT(s.shift_id) AS shifts
  , SUM(s.planned_shift_duration) AS shift_duration_sum
  , COUNT(s.planned_shift_duration) AS shift_duration_count
  , SUM(s.actual_working_time) AS actual_working_time_sum
  , COUNT(s.actual_working_time) AS actual_working_time_count
  , COUNTIF(s.shift_state = 'NO_SHOW' OR s.shift_state = 'NO_SHOW_EXCUSED' AND ARRAY_LENGTH(absences) = 0) AS no_show
  , COUNTIF(s.rider_login = 'Rider Late') AS rider_late
  , COUNTIF(s.rider_login = 'Rider Early') AS rider_early
  , COUNTIF(s.rider_login = 'Rider On Time') AS rider_on_time
FROM shift_agg s
LEFT JOIN countries c ON s.country_code = c.country_code
  AND s.starting_point_id = c.starting_point_id
WHERE s.report_date_local <  '{{ next_ds }}'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
