CREATE OR REPLACE TABLE il.schedules
PARTITION BY working_day
CLUSTER BY country_code, shift_id, rider_id AS
WITH report_dates AS (
  SELECT working_day
  FROM UNNEST(GENERATE_DATE_ARRAY(
    DATE_SUB('{{ next_ds }}', INTERVAL 6 MONTH),
    '{{ next_ds }}'
  )) AS working_day
), dataset AS (
  SELECT country_code,
    CAST(DATETIME(planned_schedule_start_time, timezone) AS DATE) AS working_day,
    zone_id,
    shift_id,
    employee_id,
    DATETIME(planned_schedule_start_time, timezone) AS planned_schedule_start_time_local,
    DATETIME(planned_schedule_end_time, timezone) AS planned_schedule_end_time_local,
    DATETIME(schedule_start_time, timezone) AS schedule_start_time_local,
    DATETIME(schedule_end_time, timezone) AS schedule_end_time_local,
    planned_schedule_start_time,
    planned_schedule_end_time,
    schedule_start_time,
    schedule_end_time,
    schedule_break_mins,
    schedule_duration_mins,
    starting_point_name,
    city_name,
    city_id,
    timezone,
    to_state,
    status,
    pending,
    email,
    ARRAY_TO_STRING(days_of_week, ',') AS days_of_week
  FROM (
    SELECT s.country_code,
      s.start_at AS planned_schedule_start_time,
      s.end_at AS planned_schedule_end_time,
      s.starting_point_id AS zone_id,
      s.id AS shift_id,
      s.employee_id,
      e.start_at  AS schedule_start_time,
      e.end_at  AS schedule_end_time,
      TIMESTAMP_DIFF(
        LAG(e.start_at, 1) over (PARTITION BY s.country_code, s.id ORDER BY e.start_at DESC),
        e.end_at,
        SECOND
      ) / 60 AS schedule_break_mins,
      TIMESTAMP_DIFF(
        e.end_at,
        e.start_at,
        SECOND
      ) / 60 AS schedule_duration_mins,
      sp.name AS starting_point_name,
      c.name AS city_name,
      c.id AS city_id,
      c.time_zone AS timezone,
      CASE WHEN e.status IS NULL AND state IN ('IN_DISCUSSION') THEN 'NO_SHOW' ELSE state END AS to_state,
      e.status,
      CASE WHEN state IN ('IN_DISCUSSION') THEN TRUE ELSE FALSE END AS pending,
      ee.email,
      days_of_week
    FROM `{{ params.project_id }}.ml.rooster_shift` s
    LEFT JOIN `{{ params.project_id }}.ml.rooster_employee` ee ON ee.id = s.employee_id
      AND s.country_code = ee.country_code
    LEFT JOIN (
      SELECT *
        , ROW_NUMBER() OVER (PARTITION BY country_code, shift_id ORDER BY created_at DESC) AS rank
      FROM `{{ params.project_id }}.ml.rooster_evaluation` e
      WHERE status NOT IN ('PENDING')
    ) e ON e.shift_id = s.id
      AND s.country_code = e.country_code
    LEFT JOIN `{{ params.project_id }}.ml.rooster_starting_point` sp ON s.starting_point_id = sp.id
      AND s.country_code = sp.country_code
    LEFT JOIN `{{ params.project_id }}.ml.rooster_city` c ON sp.city_id = c.id
      AND sp.country_code = c.country_code
    WHERE state IN ('PUBLISHED','IN_DISCUSSION','EVALUATED','NO_SHOW')
      AND CASE
            WHEN s.state = 'NO_SHOW'
              THEN (e.status IN ('PENDING', 'ACCEPTED', 'CANCELLED', 'REJECTED') OR e.status IS NULL)
            ELSE (e.status IN ('PENDING', 'ACCEPTED') OR e.status IS NULL)
          END
  )
), dataset_over_one_day AS (
  SELECT country_code,
    schedule_id,
    shift_id,
    rider_id,
    city_id,
    zone_id,
    timezone,
    days_of_week,
    CAST(DATETIME(planned_schedule_start_time, timezone) AS DATE) AS working_day,
    DATETIME(schedule_start_time, timezone) AS schedule_start_time_local,
    DATETIME(schedule_end_time, timezone) AS schedule_end_time_local,
    DATETIME(planned_schedule_start_time, timezone) AS planned_schedule_start_time_local,
    DATETIME(planned_schedule_end_time, timezone) AS planned_schedule_end_time_local,
    schedule_start_time,
    schedule_end_time,
    planned_schedule_start_time,
    planned_schedule_end_time,
    schedule_duration_mins,
    planned_schedule_duration_mins,
    schedule_break_mins,
    shift_state
  FROM (
    SELECT country_code,
      shift_id AS schedule_id,
      shift_id,
      employee_id AS rider_id,
      city_id,
      zone_id,
      timezone,
      days_of_week,
      MIN(CASE WHEN LOWER(to_state) IN ('no_show') OR CAST(planned_schedule_start_time AS DATE) > '{{ next_ds }}'
            THEN planned_schedule_start_time
            ELSE schedule_start_time
          END
      ) AS schedule_start_time,
      MAX(CASE WHEN LOWER(to_state) IN ('no_show') OR CAST(planned_schedule_start_time AS DATE) > '{{ next_ds }}'
            THEN planned_schedule_end_time
            ELSE schedule_end_time
          END
      ) AS schedule_end_time,
      MIN(planned_schedule_start_time) AS planned_schedule_start_time,
      MAX(planned_schedule_end_time) AS planned_schedule_end_time,
      SUM(IF(LOWER(to_state) NOT IN ('no_show'), schedule_duration_mins, 0)) AS schedule_duration_mins,
      MAX(TIMESTAMP_DIFF(planned_schedule_end_time, planned_schedule_start_time, SECOND)) / 60 AS planned_schedule_duration_mins,
      SUM(COALESCE(schedule_break_mins,0.0)) AS schedule_break_mins,
      MAX(LOWER(to_state)) AS shift_state
    FROM dataset
    WHERE days_of_week IS NOT NULL
    GROUP BY 1,2,3,4,5,6,7,8
    HAVING TIMESTAMP_DIFF(planned_schedule_end_time, planned_schedule_start_time, MINUTE) >= 1440
  ) AS d
), dataset_within_one_day AS (
    SELECT country_code,
      repetition_number,
      shift_id,
      rider_id,
      city_id,
      zone_id,
      timezone,
      CAST(DATETIME(planned_schedule_start_time, timezone) AS DATE) AS working_day,
      DATETIME(schedule_start_time, timezone) AS schedule_start_time_local,
      DATETIME(schedule_end_time, timezone) AS schedule_end_time_local,
      DATETIME(planned_schedule_start_time, timezone) AS planned_schedule_start_time_local,
      DATETIME(planned_schedule_end_time, timezone) AS planned_schedule_end_time_local,
      planned_schedule_start_time,
      planned_schedule_end_time,
      schedule_duration_mins,
      planned_schedule_duration_mins,
      schedule_break_mins,
      shift_state
    FROM (
      SELECT country_code,
        0 AS repetition_number,
        shift_id,
        employee_id AS rider_id,
        city_id,
        zone_id,
        timezone,
        MIN(CASE WHEN LOWER(to_state) IN ('no_show') OR CAST(planned_schedule_start_time AS DATE) > '{{ next_ds }}'
              THEN planned_schedule_start_time
              ELSE schedule_start_time
            END
        ) AS schedule_start_time,
        MAX(CASE WHEN LOWER(to_state) IN ('no_show') OR CAST(planned_schedule_start_time AS DATE) > '{{ next_ds }}'
              THEN planned_schedule_end_time
              ELSE schedule_end_time
            END
        ) AS schedule_end_time,
        MIN(planned_schedule_start_time) AS planned_schedule_start_time,
        MAX(planned_schedule_end_time) AS planned_schedule_end_time,
        SUM(IF(LOWER(to_state) NOT IN ('no_show'), schedule_duration_mins, 0)) AS schedule_duration_mins,
        MAX(TIMESTAMP_DIFF(planned_schedule_end_time, planned_schedule_start_time, SECOND)) / 60 AS planned_schedule_duration_mins,
        SUM(COALESCE(schedule_break_mins, 0.0)) AS schedule_break_mins,
        MAX(LOWER(to_state)) AS shift_state
      FROM dataset
      GROUP BY 1,2,3,4,5,6,7
      HAVING TIMESTAMP_DIFF(planned_schedule_end_time, planned_schedule_start_time, MINUTE) < 1440
    )
), final AS (
  SELECT country_code
    , repetition_number
    , shift_id
    , rider_id
    , city_id
    , zone_id
    , timezone
    , working_day
    , schedule_start_time_local
    , schedule_end_time_local
    , planned_schedule_start_time_local
    , planned_schedule_end_time_local
    , schedule_duration_mins
    , planned_schedule_duration_mins
    , schedule_break_mins
    , shift_state
  FROM dataset_within_one_day
  UNION ALL
  SELECT
    country_code,
    ROW_NUMBER() OVER(PARTITION BY country_code, shift_id ORDER BY schedule_start_time_local, schedule_end_time_local, planned_schedule_start_time_local, planned_schedule_end_time_local) AS repetition_number,
    shift_id,
    rider_id,
    city_id,
    zone_id,
    timezone,
    CASE WHEN CAST(planned_schedule_start_time_local AS DATE) = report_dates.working_day
      THEN CAST(planned_schedule_start_time_local AS DATE)
      ELSE report_dates.working_day
    END AS working_day,
    schedule_start_time_local,
    schedule_end_time_local,
    CASE WHEN CAST(planned_schedule_start_time_local AS DATE) = report_dates.working_day
      THEN planned_schedule_start_time_local
      ELSE CAST(report_dates.working_day AS DATETIME)
    END AS planned_schedule_start_time_local,
    CASE WHEN CAST(planned_schedule_end_time_local AS DATE) = report_dates.working_day
      THEN planned_schedule_end_time_local
      ELSE CAST(DATE_ADD(report_dates.working_day, INTERVAL 1 DAY) AS DATETIME)
    END AS planned_schedule_end_time_local,
    schedule_duration_mins,
    planned_schedule_duration_mins,
    schedule_break_mins,
    shift_state
  FROM dataset_over_one_day
  CROSS JOIN report_dates
  WHERE days_of_week IS NOT NULL
    AND report_dates.working_day BETWEEN CAST(planned_schedule_start_time_local AS DATE) AND CAST(planned_schedule_end_time_local AS DATE)
    AND UPPER(FORMAT_DATE("%A",
      CAST((
        CASE WHEN CAST(planned_schedule_start_time_local AS DATE) = report_dates.working_day
          THEN CAST(planned_schedule_start_time_local AS DATE)
          ELSE report_dates.working_day
        END
      ) AS DATE)
    )) IN UNNEST(SPLIT(days_of_week, ','))
)
SELECT country_code
  , repetition_number
  , shift_id
  , rider_id
  , city_id
  , zone_id
  , timezone
  , working_day
  , schedule_start_time_local
  , schedule_end_time_local
  , planned_schedule_start_time_local
  , planned_schedule_end_time_local
  , TIMESTAMP(schedule_start_time_local, timezone) AS schedule_start_time
  , TIMESTAMP(schedule_end_time_local, timezone) AS schedule_end_time
  , TIMESTAMP(planned_schedule_start_time_local, timezone) AS planned_schedule_start_time
  , TIMESTAMP(planned_schedule_end_time_local, timezone) AS planned_schedule_end_time
  , schedule_duration_mins
  , planned_schedule_duration_mins
  , schedule_break_mins
  , shift_state
FROM final
