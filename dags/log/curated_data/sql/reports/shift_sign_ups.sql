CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.shift_sign_ups`
PARTITION BY report_date_local AS
WITH base AS (
  SELECT s.country_code
    , co.country_name
    , s.rider_id
    , con.type AS contract_type
    , s.shift_id
    , s.city_id
    , ci.name AS city_name
    , s.zone_id
    , zo.name AS zone_name
    , s.starting_point_id
    , sp.name AS starting_point_name
    , DATE(s.shift_start_at, s.timezone) AS report_date_local
    -- cast as string for tableau use
	  , CAST(TIME_TRUNC(TIME(s.shift_start_at, s.timezone), SECOND) AS STRING) AS report_time_local
    , FORMAT_TIMESTAMP('%A', s.shift_start_at, s.timezone) AS shift_day_of_week
    , TIMESTAMP_DIFF(s.shift_end_at, s.shift_start_at, HOUR) AS shift_duration_hours
    , s.shift_tag
    , s.shift_state
    , sb.batch_number
    , sb.sign_up_day AS batch_sign_up_day
    , sb.percentage AS batch_percentage
    , sb.for_inactive AS batch_for_inactive
    , TIMESTAMP_DIFF(s.created_at, ssu.start_at, HOUR) AS hours_to_shift_sign_up
    , TIMESTAMP_DIFF(s.shift_end_at, s.shift_start_at, HOUR) AS planned_hours
  FROM `{{ params.project_id }}.cl.shifts` s
  LEFT JOIN `{{ params.project_id }}.cl.riders` r ON r.rider_id = s.rider_id
    AND r.country_code = s.country_code
  LEFT JOIN UNNEST(r.contracts) con ON con.city_id = s.city_id
    AND s.created_at BETWEEN con.start_at AND con.end_at
    AND con.status = 'VALID'
  LEFT JOIN UNNEST(r.batches) b ON s.created_at BETWEEN b.active_from AND b.active_until
  LEFT JOIN `{{ params.project_id }}.cl._shift_batches` sb ON sb.country_code = s.country_code
    AND sb.city_id = s.city_id
    AND sb.batch_number = b.number
  LEFT JOIN UNNEST(shift_sign_up) ssu ON TIMESTAMP_TRUNC(s.created_at, MINUTE) BETWEEN ssu.start_at AND ssu.end_at
  LEFT JOIN `{{ params.project_id }}.cl.countries` co ON co.country_code = s.country_code
  LEFT JOIN UNNEST(co.cities) ci ON ci.id = s.city_id
  LEFT JOIN UNNEST(ci.zones) zo ON zo.id = s.zone_id
  LEFT JOIN UNNEST(zo.starting_points) sp ON sp.id = s.starting_point_id
  WHERE s.shift_tag IN ('APPLICATION')
    AND s.created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 6 WEEK)
), actual_v_planned_time AS (
  SELECT s.country_code
    , shift_id
    , city_id
    , CEIL(s.planned_shift_duration / 60) AS planned_minutes
    , CEIL(eval.duration / 60) AS actual_minutes
  FROM `{{ params.project_id }}.cl.shifts` s
  LEFT JOIN UNNEST(s.evaluations) eval
  WHERE created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 5 WEEK)
    AND shift_state = 'EVALUATED'
    AND eval.status = 'ACCEPTED'
), actual_planned_final AS (
  SELECT country_code
    , city_id
    , shift_id
    , planned_minutes
    , SUM(actual_minutes) AS actual_minutes
  FROM actual_v_planned_time
  GROUP BY 1,2,3,4
)
SELECT base.*
  , CASE
      WHEN report_time_local BETWEEN '00:00:00' AND '03:00:00'
        THEN 'Night / 00:00 to 03:00'
      WHEN report_time_local BETWEEN '03:00:01' AND '06:00:00'
        THEN 'Late Night / 03:00 to 06:00'
      WHEN report_time_local BETWEEN '06:00:01' AND '09:00:00'
        THEN ' Morning / 06:00 to 09:00'
      WHEN report_time_local BETWEEN '09:00:01' AND '12:00:00'
        THEN 'Late Morning / 09:00 to 12:00'
      WHEN report_time_local BETWEEN '12:00:01' AND '15:00:00'
        THEN 'Afternoon / 12:00 - 15:00'
      WHEN report_time_local BETWEEN '15:00:01' AND '18:00:00'
        THEN 'Late Afternoon / 15:00 - 18:00'
      WHEN report_time_local BETWEEN '18:00:01' AND '21:00:00'
        THEN 'Evening / 18:00 - 21:00'
      WHEN report_time_local BETWEEN '21:00:01' AND '23:59:59'
        THEN 'Late Evening / 21:00 - 24:00'
      END AS shift_start_type
  , apf.planned_minutes
  , apf.actual_minutes
FROM base
LEFT JOIN actual_planned_final apf ON apf.country_code = base.country_code
  AND apf.city_id = base.city_id
  AND apf.shift_id = base.shift_id
WHERE report_date_local
  -- 4 weeks ago from the last Monday - to make sure we keep the whole week.
  BETWEEN DATE_SUB(DATE_TRUNC('{{ next_ds }}', WEEK(MONDAY)), INTERVAL 4 WEEK)
  -- Yesterday
  AND DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY)
  -- removing darkstore pickers to not impact utr as they do not have orders, only working hours
  AND base.country_code NOT LIKE '%dp%'
