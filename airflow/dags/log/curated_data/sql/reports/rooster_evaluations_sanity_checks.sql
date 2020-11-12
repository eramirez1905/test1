CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.rooster_evaluations_sanity_checks`
PARTITION BY working_day
CLUSTER BY country_code, rider_id AS
WITH evaluations AS (
  SELECT s.country_code
    , s.city_id
    , s.rider_id
    , e.day as working_day
    , SUM(e.duration) AS rooster_working_seconds
    , SUM(b.duration) AS rooster_break_seconds
  FROM `{{ params.project_id }}.cl.shifts` s
  LEFT JOIN UNNEST(s.actual_working_time_by_date) e
  LEFT JOIN UNNEST(s.actual_break_time_by_date) b ON e.day = b.day
  WHERE s.shift_state = 'EVALUATED'
  GROUP BY 1,2,3,4
), rider_working_time AS (
  SELECT country_code
    , working_day
    , rider_id
    , city_id
    , timezone
    , CAST(SUM(IF(state = 'working', COALESCE(duration, 0), 0)) * 60 AS INT64) AS hurrier_working_seconds
    , CAST(SUM(IF(state = 'break', COALESCE(duration, 0), 0)) * 60 AS INT64) AS hurrier_break_seconds
  FROM `{{ params.project_id }}.cl._rider_working_time`
  GROUP BY 1,2,3,4,5
), dataset AS (
  SELECT w.country_code
    , w.city_id
    , w.rider_id
    , w.working_day
    , s.rooster_working_seconds
    , w.hurrier_working_seconds
    , COALESCE(s.rooster_break_seconds, 0) AS rooster_break_seconds
    , w.hurrier_break_seconds
  FROM rider_working_time w
  LEFT JOIN evaluations s ON w.country_code = s.country_code
    AND s.rider_id = w.rider_id
    AND w.working_day = s.working_day
)
SELECT country_code
  , working_day
  , rider_id
  , ROUND((hurrier_working_seconds - rooster_working_seconds) / 3600, 1) AS working_hours_diff
  , ROUND((hurrier_break_seconds - rooster_break_seconds) / 3600, 1) AS break_hours_diff
  , ROUND(rooster_working_seconds + rooster_break_seconds - hurrier_working_seconds - hurrier_break_seconds, 2) AS total_seconds_diff
  , ROUND((rooster_working_seconds + rooster_break_seconds - hurrier_working_seconds - hurrier_break_seconds) / 3600, 1) AS total_hours_diff
  , ROUND(rooster_working_seconds / 3600, 1) AS rooster_working_hours
  , ROUND(hurrier_working_seconds / 3600, 1) AS hurrier_working_hours
  , ROUND(rooster_break_seconds  / 3600 , 1) AS rooster_break_hours
  , ROUND(hurrier_break_seconds / 3600, 1) AS hurrier_break_hours
  , rooster_working_seconds
  , hurrier_working_seconds
  , rooster_break_seconds
  , hurrier_break_seconds
  , ROUND(hurrier_working_seconds - rooster_working_seconds, 2) AS working_seconds_diff
  , ROUND(hurrier_break_seconds - rooster_break_seconds, 2) AS break_seconds_diff
  , city_id
FROM dataset
-- removing darkstore pickers to not impact utr as they do not have orders, only working hours
WHERE country_code NOT LIKE '%dp%'
