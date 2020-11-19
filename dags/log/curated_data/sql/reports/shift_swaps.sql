CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.shift_swaps`
PARTITION BY report_date_local AS
WITH shift_swap_requests AS (
  SELECT s.country_code
    , s.created_date
    , s.created_by
    , s.shift_id
    , sr.created_at
    , sr.status
    , sr.accepted_by
  FROM `{{ params.project_id }}.cl.shift_swap_requests` s
  LEFT JOIN UNNEST (swap_requests) sr
  -- removing darkstore pickers to not impact utr as they do not have orders, only working hours
  WHERE s.country_code NOT LIKE '%dp%'
), base AS (
  SELECT ssr.country_code
    , co.country_name
    , ssr.created_by AS rider_id
    , con.type AS contract_type
    , TIMESTAMP_DIFF('{{next_execution_date}}', con.start_at, DAY) AS rider_tenure_days
    , b.number AS batch_number
    -- date on which the swap request was made
    , DATE(ssr.created_at, s.timezone) AS report_date_local
    -- time at which the swap request was made
    , TIME(TIMESTAMP_TRUNC(ssr.created_at, SECOND), s.timezone) AS report_time_local
    , TIMESTAMP_DIFF(s.shift_start_at, ssr.created_at, HOUR) AS hours_to_shift
    , ssr.shift_id
    , s.city_id
    , ci.name AS city_name
    , s.zone_id
    , zo.name AS zone_name
    , s.starting_point_id
    , DATE(s.shift_start_at, s.timezone) AS shift_date_local
    , FORMAT_TIMESTAMP('%A', s.shift_start_at, s.timezone) AS shift_day_of_week
    , TIMESTAMP_DIFF(s.shift_end_at, s.shift_start_at, HOUR) AS shift_duration_hours
    , ssr.status AS swap_status
    , ssr.accepted_by AS swap_accepted_by
    , IF(ssr.status = 'ACCEPTED', s.shift_state, NULL) AS shift_state
  FROM shift_swap_requests ssr
  LEFT JOIN `{{ params.project_id }}.cl.shifts` s ON s.country_code = ssr.country_code
    AND s.shift_id = ssr.shift_id
  LEFT JOIN `{{ params.project_id }}.cl.riders` r ON r.rider_id = ssr.created_by
    AND r.country_code = ssr.country_code
  LEFT JOIN UNNEST(r.contracts) con ON con.city_id = s.city_id
    AND ssr.created_at BETWEEN con.start_at AND con.end_at
    AND con.status = 'VALID'
  LEFT JOIN UNNEST(r.batches) b ON ssr.created_at BETWEEN b.active_from AND b.active_until
  LEFT JOIN `{{ params.project_id }}.cl._shift_batches` sb ON sb.country_code = s.country_code
    AND sb.city_id = s.city_id
    AND sb.batch_number = b.number
  LEFT JOIN `{{ params.project_id }}.cl.countries` co ON co.country_code = ssr.country_code
  LEFT JOIN UNNEST(co.cities) ci ON ci.id = s.city_id
  LEFT JOIN UNNEST(ci.zones) zo ON zo.id = s.zone_id
  WHERE ssr.created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 5 WEEK)
), all_shifts AS (
  SELECT DATE(s.shift_start_at, ci.timezone) AS shift_date_local
    , s.country_code
    , c.country_name
    , s.city_id
    , ci.name AS city_name
    , s.zone_id
    , zo.name AS zone_name
    , COUNT(DISTINCT shift_id) AS shift_count
  FROM `{{ params.project_id }}.cl.shifts` s
  LEFT JOIN `{{ params.project_id }}.cl.countries` c ON c.country_code = s.country_code
  LEFT JOIN UNNEST(c.cities) ci ON ci.id = s.city_id
  LEFT JOIN UNNEST(ci.zones) zo ON zo.id = s.zone_id
  WHERE s.shift_state NOT IN ('CANCELLED', 'NO SHOW')
    AND s.created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 5 WEEK)
    -- removing darkstore pickers to not impact utr as they do not have orders, only working hours
    AND s.country_code NOT LIKE '%dp%'
  GROUP BY 1,2,3,4,5,6,7
)

SELECT COALESCE(base.country_code, s.country_code) AS country_code
  , COALESCE(base.country_name, s.country_name) AS country_name
  , base.rider_id
  , base.contract_type
  , base.rider_tenure_days
  , base.batch_number
  , base.report_date_local
  , base.report_time_local
  , base.hours_to_shift
  , base.shift_id
  , COALESCE(base.city_id, s.city_id) AS city_id
  , COALESCE(base.city_name, s.city_name) AS city_name
  , COALESCE(base.zone_id, s.zone_id) AS zone_id
  , COALESCE(base.zone_name, s.zone_name) AS zone_name
  , base.starting_point_id
  , base.shift_date_local
  , base.shift_day_of_week
  , base.shift_duration_hours
  , base.swap_status
  , base.swap_accepted_by
  , base.shift_state
  , CASE
      WHEN base.report_time_local BETWEEN '00:00:00' AND '03:00:00'
        THEN 'Night / 00:00 to 03:00'
      WHEN base.report_time_local BETWEEN '03:00:01' AND '06:00:00'
        THEN 'Late Night / 03:00 to 06:00'
      WHEN base.report_time_local BETWEEN '06:00:01' AND '09:00:00'
        THEN ' Morning / 06:00 to 09:00'
      WHEN base.report_time_local BETWEEN '09:00:01' AND '12:00:00'
        THEN 'Late Morning / 09:00 to 12:00'
      WHEN base.report_time_local BETWEEN '12:00:01' AND '15:00:00'
        THEN 'Afternoon / 12:00 - 15:00'
      WHEN base.report_time_local BETWEEN '15:00:01' AND '18:00:00'
        THEN 'Late Afternoon / 15:00 - 18:00'
      WHEN base.report_time_local BETWEEN '18:00:01' AND '21:00:00'
        THEN 'Evening / 18:00 - 21:00'
      WHEN base.report_time_local BETWEEN '21:00:01' AND '23:59:59'
        THEN 'Late Evening / 21:00 - 24:00'
    END AS shift_start_type
  , s.shift_count AS total_shift_count
FROM base
FULL OUTER JOIN all_shifts s ON base.country_code = s.country_code
  AND base.city_id = s.city_id
  AND base.zone_id = s.zone_id
  AND base.shift_date_local = s.shift_date_local
WHERE report_date_local
  -- 4 weeks ago from the last Monday - to make sure we keep the whole week.
  BETWEEN DATE_SUB(DATE_TRUNC('{{ next_ds }}', WEEK(MONDAY)), INTERVAL 4 WEEK)
    -- Yesterday
    AND DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY)
