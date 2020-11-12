CREATE OR REPLACE TABLE il.shifts
PARTITION BY created_date AS
WITH countries AS (
  SELECT co.country_code
    , co.country_name
    , ci.id AS city_id
    , ci.name AS city_name
    , ci.timezone
    , z.id AS zone_id
    , z.name AS zone_name
    , sp.id AS starting_point_id
    , sp.name AS starting_point_name
  FROM `{{ params.project_id }}.cl.countries` co
  LEFT JOIN UNNEST(cities) ci
  LEFT JOIN UNNEST(ci.zones) z
  LEFT JOIN UNNEST(z.starting_points) sp
), rooster_pending_evaluation AS (
/********* Take all evaluated shifts ********/
  SELECT DISTINCT s.country_code
    , s.id AS shift_id
    , e.id AS evaluation_id
    , e.start_at
    , e.end_at
    , e.status
  FROM `{{ params.project_id }}.ml.rooster_shift` s
  LEFT JOIN `{{ params.project_id }}.ml.rooster_evaluation` e ON s.country_code = e.country_code
    AND s.id = e.shift_id
  WHERE e.status NOT IN ('PENDING')
), rooster_dataset AS (
  SELECT s.country_code
    , co.city_id
    , co.city_name
    , co.timezone
    , co.zone_id
    , s.starting_point_id
    , s.id AS shift_id
    , s.employee_id
    , CAST(s.start_at AS DATE) AS shift_start_date
    , s.start_at AS shift_start_time
    , s.end_at AS shift_end_time
    , CASE
        WHEN co.starting_point_name LIKE '%NO*UTR%'
          THEN TRUE
        ELSE FALSE
      END AS no_utr
    , CASE
        WHEN s.state IN ('IN_DISCUSSION') AND e.status IS NULL
          THEN 'NO_SHOW'
        ELSE s.state
      END AS to_state
    , CASE
        WHEN s.state IN ('IN_DISCUSSION')
          THEN TRUE
        ELSE FALSE
      END AS pending
    , weekday
    , CAST(s.tag AS STRING) AS tag
    , s.created_at
    , s.parent_id
    , e.start_at AS evaluation_start_time
    , e.end_at AS evaluation_end_time
    , CAST(TIMESTAMP_DIFF(LAG(e.start_at, 1) OVER (PARTITION BY s.country_code, s.id ORDER BY e.start_at DESC), e.end_at, SECOND) / 60 AS FLOAT64) AS evaluation_break_mins
    , CAST(TIMESTAMP_DIFF(e.end_at, e.start_at, SECOND) / 60 AS FLOAT64) AS evaluation_duration_mins
    , e.status
  FROM `{{ params.project_id }}.ml.rooster_shift` s
  CROSS JOIN UNNEST (s.days_of_week) AS weekday
  LEFT JOIN `{{ params.project_id }}.ml.rooster_employee` ee ON s.country_code = ee.country_code
    AND s.employee_id = ee.id
  LEFT JOIN rooster_pending_evaluation e ON s.country_code = e.country_code
    AND s.id = e.shift_id
  LEFT JOIN countries co ON s.country_code = co.country_code
    AND s.starting_point_id = co.starting_point_id
  WHERE s.state IN ('PUBLISHED', 'IN_DISCUSSION', 'EVALUATED', 'NO_SHOW')
    AND CASE
          WHEN s.state = 'NO_SHOW'
            THEN (e.status IN ('PENDING', 'ACCEPTED', 'CANCELLED', 'REJECTED')
              OR e.status IS NULL)
          ELSE (e.status IN ('PENDING', 'ACCEPTED')
            OR e.status IS NULL)
        END
), calc AS (
  SELECT rd.country_code
    , rd.city_id
    , rd.city_name
    , rd.timezone
    , zone_id
    , starting_point_id
    , rd.shift_id
    , employee_id
    , parent_id
    , no_utr
    , tag
    , weekday
    , CASE
        WHEN MAX(parent_id) IS NOT NULL
          THEN TRUE
        ELSE FALSE
      END AS repeating
    , MIN(shift_start_date) AS shift_start_date
    , MIN(rd.shift_start_time) AS shift_start_time
    , MAX(rd.shift_end_time) AS shift_end_time
    , MAX(CAST(TIMESTAMP_DIFF(rd2.shift_end_time, rd2.shift_start_time, MINUTE) AS FLOAT64)) AS shift_duration_min
    , MAX(LOWER(to_state)) AS shift_state
    , CAST(MIN(rd.shift_start_time) AS DATE) AS evaluation_start_date
    , MIN(CASE
            WHEN to_state LIKE 'NO_SHOW'
              THEN rd.shift_start_time
            WHEN CAST(rd.shift_start_time AS DATE) > CURRENT_DATE
              THEN rd.shift_start_time
            ELSE evaluation_start_time
          END) AS evaluation_start_time
    , MAX(CASE
            WHEN to_state LIKE 'NO_SHOW'
              THEN rd.shift_end_time
            WHEN CAST(rd.shift_start_time AS DATE) > CURRENT_DATE
              THEN rd.shift_end_time
            ELSE evaluation_end_time
          END) AS evaluation_end_time
    , SUM(evaluation_duration_mins) AS evaluation_duration_min
    , SUM(COALESCE(evaluation_break_mins, 0.0)) AS evaluation_break_min
    , MIN(created_at) AS created_at
  FROM rooster_dataset rd
  LEFT JOIN (SELECT country_code
               , city_id
               , shift_id
               , MIN(shift_start_time) AS shift_start_time
               , MAX(shift_end_time) AS shift_end_time
             FROM rooster_dataset
             WHERE to_state != 'NO_SHOW'
             GROUP BY 1, 2, 3) rd2 USING(country_code, city_id, shift_id)
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
), repeated_shifts AS (
  SELECT country_code
    , shift_id
    , shift_start_date
    , weekday
    , GENERATE_DATE_ARRAY(shift_start_date, CAST(shift_end_time AS DATE), INTERVAL 1 DAY) AS time_series_array
  FROM calc
  WHERE repeating IS TRUE
)
SELECT DISTINCT country_code
  , city_id
  , city_name
  , timezone
  , zone_id
  , starting_point_id
  , shift_id
  , employee_id AS courier_id
  , 0 AS repetition_number
  , repeating
  , parent_id
  , no_utr
  , shift_state
  , shift_state = 'no_show' AS no_show
  , tag
  , shift_start_date
  , shift_start_time
  , shift_end_time
  , shift_duration_min
  , evaluation_start_date
  , evaluation_start_time
  , evaluation_end_time
  , evaluation_duration_min
  , evaluation_break_min
  , TIMESTAMP_DIFF(shift_start_time, evaluation_start_time, SECOND) / 60 AS login_diff_min
  , TIMESTAMP_DIFF(shift_end_time, evaluation_end_time, SECOND) / 60 AS logout_diff_min
  , CASE
      WHEN evaluation_end_time > shift_end_time
        AND shift_state != 'no_show'
        THEN TRUE
      ELSE FALSE
    END AS late_logout
  , CASE
      WHEN shift_end_time > evaluation_end_time
        AND shift_state != 'no_show'
        THEN TRUE
      ELSE FALSE
    END AS early_logout
  , CASE
      WHEN evaluation_start_time > shift_start_time
        AND shift_state != 'no_show'
        THEN TRUE
      ELSE FALSE
    END AS late_login
  , CASE
      WHEN evaluation_start_time < shift_start_time
        AND shift_state != 'no_show'
        THEN TRUE
      ELSE FALSE
    END AS early_login
  , created_at
  , CAST(created_at AS DATE) AS created_date
FROM calc
WHERE repeating IS FALSE

UNION ALL

SELECT DISTINCT r.country_code
  , c.city_id
  , city_name
  , timezone
  , zone_id
  , starting_point_id
  , r.shift_id
  , employee_id AS courier_id
  , ROW_NUMBER() OVER (PARTITION BY r.country_code, r.shift_id) AS repetition_number
  , repeating
  , parent_id
  , no_utr
  , shift_state
  , shift_state = 'no_show' AS no_show
  , tag
  , CASE
      WHEN r.shift_start_date = time_series_date
        THEN r.shift_start_date
      ELSE time_series_date
    END AS shift_start_date
  , CASE
      WHEN r.shift_start_date = time_series_date
        THEN shift_start_time
      ELSE CAST(time_series_date AS TIMESTAMP)
    END AS shift_start_time
  , shift_end_time
  , shift_duration_min
  , CASE
      WHEN r.shift_start_date = time_series_date
        THEN r.shift_start_date
      ELSE time_series_date
    END AS evaluation_start_date
  , evaluation_start_time
  , evaluation_end_time
  , evaluation_duration_min
  , evaluation_break_min
  , TIMESTAMP_DIFF(shift_start_time, evaluation_start_time, SECOND) / 60 AS login_diff_min
  , TIMESTAMP_DIFF(shift_end_time, evaluation_end_time, SECOND) / 60 AS logout_diff_min
  , CASE
      WHEN evaluation_end_time > shift_end_time
        AND shift_state != 'no_show'
        THEN TRUE
      ELSE FALSE
    END AS late_logout
  , CASE
      WHEN shift_end_time > evaluation_end_time
        AND shift_state != 'no_show'
        THEN TRUE
      ELSE FALSE
    END AS early_logout
  , CASE
      WHEN evaluation_start_time > shift_start_time
        AND shift_state != 'no_show'
        THEN TRUE
      ELSE FALSE
    END AS late_login
  , CASE
      WHEN evaluation_start_time < shift_start_time
        AND shift_state != 'no_show'
        THEN TRUE
      ELSE FALSE
    END AS early_login
  , created_at
  , CAST(created_at AS DATE) AS created_date
FROM repeated_shifts r
CROSS JOIN UNNEST(r.time_series_array) AS time_series_date
LEFT JOIN calc c ON r.country_code = c.country_code
  AND r.shift_id = c.shift_id
  AND r.shift_start_date = c.shift_start_date
WHERE LOWER(FORMAT_DATE('%A', (CASE
                                 WHEN r.shift_start_date = time_series_date
                                   THEN r.shift_start_date
                                 ELSE time_series_date
                               END))) = LOWER(r.weekday)
