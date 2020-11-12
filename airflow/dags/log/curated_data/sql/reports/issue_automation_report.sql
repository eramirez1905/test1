CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.issue_automation_report`
PARTITION BY report_date
CLUSTER BY country_name AS
WITH dates AS (
  SELECT date
  FROM UNNEST(
    GENERATE_TIMESTAMP_ARRAY(
      TIMESTAMP_TRUNC(TIMESTAMP_SUB('{{ next_ds }}', INTERVAL 45 DAY), DAY)
      , TIMESTAMP_TRUNC(TIMESTAMP_ADD('{{ next_ds }}', INTERVAL 2 HOUR), HOUR)
      , INTERVAL 30 MINUTE)
    ) AS date
), report_time_windows_with_zone AS (
  SELECT CAST(date AS DATE) AS report_date_window
    , date AS start_time_window
    , TIMESTAMP_ADD(date, INTERVAL 30 MINUTE) AS end_time_window
    , c.country_name
    , c.country_code
    , ci.name AS city_name
    , ci.id AS city_id
    , z.name AS zone_name
    , z.id AS zone_id
    , ci.timezone
  FROM dates
  CROSS JOIN `{{ params.project_id }}.cl.countries` c
  LEFT JOIN UNNEST(cities) ci
  LEFT JOIN UNNEST(zones) z
  -- removing darkstore pickers to not impact utr as they do not have orders, only working hours
  WHERE c.country_code NOT LIKE '%dp%'
), report_time_window AS (
  SELECT CAST(date AS DATE) AS report_date_window_simple
    , date AS start_time_window_simple
    , TIMESTAMP_ADD(date, INTERVAL 30 MINUTE) AS end_time_window_simple
  FROM dates
), delivery_status AS (
  SELECT o.country_code
    , d.id AS delivery_id
    , d.delivery_status
  FROM `{{ params.project_id }}.cl.orders` o
  LEFT JOIN UNNEST(o.deliveries) d
  -- removing darkstore pickers to not impact utr as they do not have orders, only working hours
  WHERE o.country_code NOT LIKE '%dp%'
), issues_base AS (
  SELECT i.issue_id
    , i.country_code
    , co.country_name
    , i.city_id
    , ci.name AS city_name
    , i.zone_id
    , zo.name AS zone_name
    , i.delivery_id
    , ds.delivery_status
    , i.rider_id
    , i.issue_type
    , i.issue_category
    , i.issue_name
    , i.timezone IS NOT NULL AS has_timezone
    , i.resolved_at IS NOT NULL AS is_resolved
    , i.resolved_by
     -- needed to calculate the resolution time to get unique values.
    , ROW_NUMBER() OVER (PARTITION BY i.issue_id, i.country_code ORDER BY a.time_to_trigger_minutes, a.action) AS rank
    , a.action_id
    , a.time_to_trigger_minutes
    , a.action
    , a.created_at
    , a.updated_at
    , i.timezone
    , LOWER(c.type) AS condition_type
    , LOWER(c.value) AS condition_value
    , i.show_on_watchlist
    , TIMESTAMP_DIFF(i.resolved_at, i.created_at, MINUTE) AS time_to_resolution_minutes
  FROM `{{ params.project_id }}.cl.issues` i
  LEFT JOIN UNNEST(actions) a
  LEFT JOIN UNNEST(a.conditions) c
  LEFT JOIN `{{ params.project_id }}.cl.countries` co ON co.country_code = i.country_code
  LEFT JOIN UNNEST(cities) ci ON ci.id = i.city_id
  LEFT JOIN UNNEST(ci.zones) zo ON zo.id = i.zone_id
  LEFT JOIN delivery_status ds ON ds.country_code = i.country_code
    AND ds.delivery_id = i.delivery_id
  WHERE i.created_date >= DATE_SUB(DATE_TRUNC('{{ next_ds }}', ISOWEEK), INTERVAL 5 WEEK)
    AND i.is_automated IS TRUE
    -- in hurrier_issues we had ghost_rider while in issue_service this doesn't exist anymore
    AND (i.is_ghost_rider IS FALSE OR i.is_ghost_rider IS NULL)
    -- removing darkstore pickers to not impact utr as they do not have orders, only working hours
    AND i.country_code NOT LIKE '%dp%'
)
SELECT base.issue_id
  , COALESCE(time.country_code, base.country_code) AS country_code
  , COALESCE(time.country_name, base.country_name) AS country_name
  , base.delivery_id
  , base.delivery_status
  , base.rider_id
  , base.issue_type
  , base.issue_category
  , base.issue_name
  , base.has_timezone
  , base.resolved_by
  , base.rank
  , base.action_id
  , base.time_to_trigger_minutes
  , base.action
  , COALESCE(DATE(time.start_time_window, time.timezone), DATE(base.created_at, base.timezone)) AS report_date
  , CAST(TIME(base.created_at, base.timezone) AS STRING) AS report_time_local
  , base.condition_type
  , base.condition_value
  , base.show_on_watchlist
  , base.time_to_resolution_minutes
  , COALESCE(CAST(TIME(time.start_time_window, time.timezone) AS STRING), CAST(TIME(t.start_time_window_simple, base.timezone) AS STRING)) AS start_time_window
  , COALESCE(CAST(TIME(time.end_time_window, time.timezone) AS STRING), CAST(TIME(t.end_time_window_simple, base.timezone) AS STRING)) AS end_time_window
  , COALESCE(time.city_name, base.city_name) AS city_name
  , COALESCE(time.city_id, base.city_id) AS city_id
  , COALESCE(time.zone_name, base.zone_name) AS zone_name
  , COALESCE(time.zone_id, base.zone_id) AS zone_id
FROM report_time_windows_with_zone time
FULL OUTER JOIN issues_base base ON time.report_date_window = DATE(base.created_at)
  AND time.start_time_window <= base.created_at
  AND time.end_time_window > base.created_at
  AND time.country_name = base.country_name
  AND time.country_code = base.country_code
  AND time.city_id = base.city_id
  AND time.zone_id = base.zone_id
LEFT JOIN report_time_window t ON t.report_date_window_simple = DATE(base.created_at)
  AND base.created_at BETWEEN t.start_time_window_simple AND t.end_time_window_simple
