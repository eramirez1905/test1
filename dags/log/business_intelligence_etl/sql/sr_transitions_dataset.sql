CREATE OR REPLACE TABLE il.sr_transitions_dataset
PARTITION BY report_date_local AS
WITH parameters AS (
  SELECT DATE_SUB(DATE_SUB('{{ next_ds }}', INTERVAL 5 WEEK), INTERVAL 1 DAY) AS start_date
    , DATE_ADD('{{ next_ds }}', INTERVAL 2 WEEK) AS end_date
), dates AS (
  SELECT date_time
    FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(
    TIMESTAMP_TRUNC(TIMESTAMP_SUB((SELECT CAST(end_date AS TIMESTAMP) FROM parameters), INTERVAL 50 DAY), DAY),
    TIMESTAMP_TRUNC((SELECT CAST(end_date AS TIMESTAMP) FROM parameters), HOUR), INTERVAL 15 MINUTE)
  ) AS date_time
), report_dates AS (
  SELECT CAST(date_time AS DATE) AS report_date
    , date_time AS start_datetime
    , TIMESTAMP_ADD(date_time, INTERVAL 15 MINUTE) AS end_datetime
  FROM dates
), rider_working_time AS (
  SELECT country_code
    , working_day
    , state
    , rider_id
    , city_id
    , starting_point_id
    , timezone
    , TIMESTAMP(started_at_local, timezone) AS started_at
    , TIMESTAMP(ended_at_local, timezone) AS ended_at
    , duration
  FROM `{{ params.project_id }}.cl._rider_working_time`
  WHERE working_day BETWEEN (SELECT start_date FROM parameters) AND (SELECT end_date FROM parameters)
)
SELECT e.country_code
  , e.city_id
  , CAST(DATETIME(dates.start_datetime, e.timezone) AS DATE) AS report_date_local
  , DATETIME(dates.start_datetime, e.timezone) AS start_datetime_local
  , DATETIME(dates.end_datetime, e.timezone) AS end_datetime_local
  , sp.geo_zone_id AS zone_id
  , e.timezone
  , SUM(IF(state IN ('working'),
      TIMESTAMP_DIFF(LEAST(dates.end_datetime, e.ended_at), GREATEST(e.started_at, dates.start_datetime), SECOND) / 60 / 60,
      0)
    ) AS trans_working
  , SUM(IF(state IN ('break'),
      TIMESTAMP_DIFF(LEAST(dates.end_datetime, e.ended_at), GREATEST(e.started_at, dates.start_datetime), SECOND) / 60 / 60,
      0)
    ) AS trans_break
  , SUM(IF(state IN ('busy'),
      TIMESTAMP_DIFF(LEAST(dates.end_datetime, e.ended_at), GREATEST(e.started_at, dates.start_datetime), SECOND) / 60 / 60,
      0)
    ) AS trans_busy_time
  , SUM(IF(state IN ('temp_not_working'),
      TIMESTAMP_DIFF(LEAST(dates.end_datetime, e.ended_at), GREATEST(e.started_at, dates.start_datetime), SECOND) / 60 / 60,
      0)
    ) AS trans_temp_not_working
FROM rider_working_time e
INNER JOIN report_dates dates ON dates.start_datetime < e.ended_at
  AND e.started_at < dates.end_datetime
LEFT JOIN `{{ params.project_id }}.ml.rooster_starting_point` sp ON e.starting_point_id = sp.id
  AND e.country_code = sp.country_code
WHERE duration > 0
  AND e.working_day BETWEEN (SELECT start_date FROM parameters) AND (SELECT end_date FROM parameters)
GROUP BY 1, 2, 3, 4, 5, 6, 7
;
