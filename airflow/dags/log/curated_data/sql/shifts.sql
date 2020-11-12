CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.shifts`
PARTITION BY created_date
CLUSTER BY country_code, rider_id, shift_id, shift_state AS
WITH countries AS (
  SELECT country_code
    , ci.id AS city_id
    , ci.timezone AS timezone
    , zo.id AS zone_id
    , sp.id AS starting_point_id
  FROM `{{ params.project_id }}.cl.countries` co
  LEFT JOIN UNNEST(co.cities) ci
  LEFT JOIN UNNEST(ci.zones) zo
  LEFT JOIN UNNEST (zo.starting_points) sp
), rooster_shifts AS (
  SELECT *
  FROM `{{ params.project_id }}.dl.rooster_shift`
), rooster_evaluations AS (
  SELECT *
  FROM `{{ params.project_id }}.dl.rooster_evaluation`
), rider_working_time AS (
  SELECT *
  FROM `{{ params.project_id }}.cl._rider_working_time`
), orders AS (
  SELECT *
  FROM `{{ params.project_id }}.cl.orders`
), riders AS (
  SELECT *
  FROM `{{ params.project_id }}.cl.riders`
), vehicle_dataset AS (
  SELECT country_code
    , rider_id
    , id
    , CAST(rider_dropped_off_at AS DATE) AS report_date
    , vehicle.vehicle_bag
    , vehicle.profile AS vehicle_profile
  FROM orders o
  LEFT JOIN UNNEST (deliveries) d
  WHERE d.delivery_status = 'completed'
), vehicle_most_orders_ordered AS (
  SELECT country_code
    , rider_id
    , report_date
    , vehicle_bag
    , vehicle_profile
    , ROW_NUMBER() OVER (PARTITION BY country_code, rider_id, report_date ORDER BY COUNT(DISTINCT id) DESC) AS row_number
    , COUNT(id) AS deliveries
  FROM vehicle_dataset
  GROUP BY 1,2,3,4,5
), vehicle_most_orders AS (
  SELECT * EXCEPT (row_number, deliveries)
  FROM vehicle_most_orders_ordered
  WHERE row_number = 1
), evaluations_dataset AS (
    SELECT country_code
      , shift_id
      , rider_id
      , timezone
      , city_id
      , zone_id
      , ARRAY(
          SELECT AS STRUCT status
            , id
            , vehicle
            , start_at_local
            , end_at_local
            , TIMESTAMP(start_at_local, timezone) AS start_at
            , TIMESTAMP(end_at_local, timezone) AS end_at
          FROM UNNEST(evaluations)
        ) AS evaluations
    FROM (
      SELECT e.country_code
        , e.shift_id
        , s.employee_id AS rider_id
        , c.timezone
        , c.city_id
        , c.zone_id
        , ARRAY_AGG(STRUCT(e.id
            , e.status
            , e.vehicle_type_id AS rooster_vehicle_type_id
            , STRUCT(ve.name) AS vehicle
            , IF(real_working_day = DATE(e.start_at, timezone),
                DATETIME(e.start_at, timezone),
                DATETIME(TIMESTAMP(real_working_day, timezone), timezone)
              ) AS start_at_local
            , IF(real_working_day >= DATE(e.end_at, timezone),
                DATETIME(e.end_at, timezone),
                DATETIME(TIMESTAMP(DATE_ADD(real_working_day, INTERVAL 1 DAY), timezone), timezone)
              ) AS end_at_local
          )) AS evaluations
      FROM rooster_evaluations e
      LEFT JOIN rooster_shifts s ON s.country_code = e.country_code
        AND s.id = e.shift_id
      LEFT JOIN countries c ON c.country_code = s.country_code
        AND s.starting_point_id = c.starting_point_id
      LEFT JOIN `{{ params.project_id }}.dl.rooster_vehicle_type` ve ON ve.country_code = e.country_code
        AND ve.id = e.vehicle_type_id
      LEFT JOIN UNNEST(GENERATE_DATE_ARRAY(DATE(e.start_at, timezone), DATE(e.end_at, timezone))) real_working_day
      GROUP BY 1,2,3,4,5,6
    )
), break_time_raw AS (
    SELECT country_code
      , shift_id
      , rider_id
      , IF(real_working_day = DATE(start_at_local),
          start_at_local,
          DATETIME(TIMESTAMP(real_working_day, timezone), timezone)
        ) AS start_at_local
      , IF(real_working_day >= DATE(end_at_local),
          end_at_local,
          DATETIME(TIMESTAMP(DATE_ADD(real_working_day, INTERVAL 1 DAY), timezone), timezone)
        ) AS end_at_local
      , timezone
    FROM (
      SELECT ed.country_code
        , ed.rider_id
        , ed.shift_id
        , e.end_at_local AS start_at_local
        , LEAD(e.start_at_local) OVER (PARTITION BY ed.country_code, ed.shift_id ORDER BY e.start_at_local) AS end_at_local
        , ed.timezone
      FROM evaluations_dataset ed
      LEFT JOIN UNNEST(evaluations) e
      WHERE e.status IN ('ACCEPTED')
    )
    LEFT JOIN UNNEST(GENERATE_DATE_ARRAY(DATE(start_at_local), DATE(end_at_local))) real_working_day
    WHERE end_at_local IS NOT NULL
      -- In case there are no breaks, start_at = end_at
      AND start_at_local != end_at_local
), break_time AS (
  SELECT b.country_code
    , b.shift_id
    , ARRAY_AGG(
        STRUCT(
            start_at_local
          , end_at_local
          , TIMESTAMP(start_at_local, b.timezone) AS start_at
          , TIMESTAMP(end_at_local, b.timezone) AS end_at
          , t.details
        )
      ) AS timings
    , ARRAY_AGG(
        STRUCT(
            start_at_local AS rooster_start_at_local
          , end_at_local AS rooster_end_at_local
          , t.started_at_local AS hurrier_started_at_local
          , t.ended_at_local AS hurrier_ended_at_local
          , TIMESTAMP(start_at_local, b.timezone) AS rooster_start_at
          , TIMESTAMP(end_at_local, b.timezone) AS rooster_end_at
          , t.started_at AS hurrier_started_at
          , t.ended_at AS hurrier_ended_at
        )
      ) AS _timings_hurrier
  FROM break_time_raw b
  LEFT JOIN rider_working_time t ON t.country_code = b.country_code
    AND t.rider_id = b.rider_id
    AND DATETIME_TRUNC(t.started_at_local, MILLISECOND) >= DATETIME_TRUNC(b.start_at_local, MILLISECOND)
    AND DATETIME_TRUNC(t.ended_at_local, MILLISECOND) <= DATETIME_TRUNC(b.end_at_local, MILLISECOND)
    AND t.state = 'break'
  GROUP BY 1, 2
), shift_dataset AS (
  SELECT s.country_code
    , CAST(s.start_at AS DATE) AS created_date
    , s.id AS shift_id
    , s.employee_id AS rider_id
    , c.city_id
    , c.timezone
    , s.starting_point_id
    , c.zone_id
    , s.created_by AS shift_created_by
    , s.updated_by AS shift_updated_by
    , s.created_at
    , s.updated_at
    , s.start_at AS shift_start_at
    , s.end_at AS shift_end_at
    , s.state AS shift_state
    , s.days_of_week
    , CASE WHEN s.parent_id IS NOT NULL OR TIMESTAMP_DIFF(s.end_at, s.start_at, DAY) > 1
        THEN TRUE
        ELSE FALSE
      END AS is_repeating
    , s.tag AS shift_tag
    , (SELECT MIN(e.start_at) FROM UNNEST(evaluations) AS e) AS actual_start_at
    , (SELECT MAX(e.end_at) FROM UNNEST(evaluations) AS e) AS actual_end_at
    , ARRAY((
        SELECT AS STRUCT *
          , COALESCE(TIMESTAMP_DIFF(e.end_at, e.start_at, SECOND), 0) AS duration
        FROM UNNEST(e.evaluations) AS e
        ORDER BY start_at, end_at
      )) AS evaluations
    , ARRAY((
        SELECT AS STRUCT *
          , TIMESTAMP_DIFF(b.end_at, b.start_at, SECOND) AS duration
        FROM UNNEST(b.timings) AS b
        ORDER BY start_at, end_at
      )) AS break_time
    , ARRAY((
        SELECT AS STRUCT *
          , TIMESTAMP_DIFF(b.rooster_end_at, b.rooster_start_at, SECOND) AS duration
          , TIMESTAMP_DIFF(b.hurrier_ended_at, b.hurrier_started_at, SECOND) AS hurrier_duration
        FROM UNNEST(b._timings_hurrier) AS b
        ORDER BY start_at, end_at
      )) AS _break_time_hurrier
  FROM rooster_shifts s
  LEFT JOIN evaluations_dataset e ON s.country_code = e.country_code
    AND s.id = e.shift_id
  LEFT JOIN break_time b ON e.country_code = b.country_code
    AND e.shift_id = b.shift_id
  LEFT JOIN countries c ON c.country_code = s.country_code
    AND s.starting_point_id = c.starting_point_id
), absences AS (
  SELECT r.country_code
    , r.rider_id
    , a.id
    , a.start_at
    , a.end_at
    , a.status
    , a.is_paid
    , a.reason
    , a.comment
    , a.violation_id
  FROM riders r
  LEFT JOIN UNNEST (absences_history) a
  WHERE a.status = 'ACCEPTED'
), absences_overlapping_shifts AS (
  SELECT s.country_code
    , s.shift_id
    , ARRAY_AGG(
        STRUCT(a.id
          , a.start_at
          , a.end_at
          , a.status
          , a.is_paid
          , a.reason
          , a.comment
          , a.violation_id
        )
     ) AS absences
 FROM shift_dataset s
 INNER JOIN absences a ON s.rider_id = a.rider_id
   AND s.country_code = a.country_code
   AND a.start_at <= s.shift_end_at
   AND s.shift_start_at <= a.end_at
 GROUP BY 1,2
), deliveries_dataset AS (
  SELECT country_code
    , shift_id
    , STRUCT(SUM(accepted_deliveries_count) AS accepted
        , SUM(notified_deliveries_count) AS notified
      ) AS deliveries
  FROM rooster_evaluations
  GROUP BY 1, 2
)
SELECT s.country_code
  , s.created_date
  , s.shift_id
  , s.rider_id
  , s.city_id
  , s.timezone
  , s.starting_point_id
  , s.zone_id
  , s.is_repeating
  , s.shift_start_at
  , s.shift_end_at
  , TIMESTAMP_DIFF(s.shift_end_at, s.shift_start_at, SECOND) AS planned_shift_duration
  , s.shift_state
  , a.absences
  , s.shift_tag
  , s.actual_start_at
  , s.actual_end_at
  , TIMESTAMP_DIFF(s.actual_start_at, s.shift_start_at, SECOND) AS login_difference
  , TIMESTAMP_DIFF(s.actual_end_at, s.shift_end_at, SECOND) AS logout_difference
  , d.deliveries
  , s.days_of_week
  , COALESCE((SELECT SUM(e.duration) FROM UNNEST(evaluations) AS e WHERE status = 'ACCEPTED'), 0) AS actual_working_time
  , ARRAY((SELECT AS STRUCT DATE(start_at, timezone) AS day
      , status
      , vehicle.name AS vehicle_name
      , CAST(NULL AS INT64) AS accepted_deliveries_count
      , CAST(NULL AS INT64) AS notified_deliveries_count
      , COALESCE(SUM(e.duration), 0) AS duration
    FROM UNNEST(evaluations) AS e
    GROUP BY 1, 2, 3
  )) AS actual_working_time_by_date
  , COALESCE((SELECT SUM(e.duration) FROM UNNEST(break_time) AS e), 0) AS actual_break_time
  , ARRAY((SELECT AS STRUCT DATE(start_at, timezone) AS day, COALESCE(SUM(e.duration), 0) AS duration FROM UNNEST(break_time) AS e GROUP BY 1)) AS actual_break_time_by_date
  , evaluations
  , s.break_time
  , ARRAY((
      SELECT AS STRUCT *
        , b.duration - b.hurrier_duration AS duration_diff
      FROM UNNEST(s._break_time_hurrier) AS b
      ORDER BY hurrier_started_at, hurrier_ended_at
    )) AS _break_time_hurrier
  , s.shift_created_by
  , s.shift_updated_by
  , s.created_at
  , s.updated_at
  , v.vehicle_bag
  , v.vehicle_profile
FROM shift_dataset s
LEFT JOIN vehicle_most_orders v ON s.country_code = v.country_code
  AND s.rider_id = v.rider_id
  AND CAST(s.actual_start_at AS DATE) = v.report_date
LEFT JOIN absences_overlapping_shifts a ON s.shift_id = a.shift_id
  AND s.country_code = a.country_code
LEFT JOIN deliveries_dataset d ON s.country_code = d.country_code
  AND s.shift_id = d.shift_id
