CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.closures_shrinkages_report`
PARTITION BY DATE(start_datetime) AS
WITH dates AS (
  SELECT date
  FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(
    TIMESTAMP_TRUNC(TIMESTAMP_SUB('{{ next_execution_date }}', INTERVAL 70 DAY), DAY), 
    TIMESTAMP_TRUNC('{{ next_execution_date }}', HOUR), 
    INTERVAL 30 MINUTE)
  ) AS date
), report_dates AS (
  SELECT CAST(date AS DATE) AS report_date
    , date AS start_datetime
    , TIMESTAMP_ADD(date, INTERVAL 30 MINUTE) AS end_datetime
  FROM dates
), country_timezones AS (
  SELECT DISTINCT co.country_code
    , FIRST_VALUE(ci.timezone) OVER (PARTITION BY country_code ORDER BY ci.timezone) AS timezone
  FROM `{{ params.project_id }}.cl.countries` co
  LEFT JOIN UNNEST(cities) ci
), events AS (
  SELECT p.country_code
    , co.country_name
    , p.city_id
    , COALESCE(ci.name, 'Unknown') AS city_name
    , t.zone_id
    , CASE
        WHEN t.zone_id = 0
          THEN 'Unknown'
        ELSE z.name
      END AS zone_name
    , p.platform
    , CAST(DATETIME(t.start_at, COALESCE(p.timezone, ct.timezone)) AS DATE) AS report_date
    , t.duration / 60 AS event_duration
    , TIMESTAMP(DATETIME(t.start_at, COALESCE(p.timezone, ct.timezone))) AS starts_at_local
    , TIMESTAMP(DATETIME(t.end_at, COALESCE(p.timezone, ct.timezone))) AS ends_at_local
    , TIME(DATETIME(t.start_at, COALESCE(p.timezone, ct.timezone))) AS event_start_time_local
    , TIME(DATETIME(t.end_at, COALESCE(p.timezone, ct.timezone))) AS event_end_time_local
    , CONCAT(CAST(event_id AS STRING), '-', CAST(t.transaction_id AS STRING)) AS event_id
    , t.title AS event_title
    , is_halal AS halal
    , t.value
    , t.action
    , CASE
        WHEN FORMAT_DATE('%G-%V', CAST(DATETIME(t.start_at, COALESCE(p.timezone, ct.timezone)) AS DATE)) = FORMAT_DATE('%G-%V', '{{ next_ds }}')
          THEN 'current_week'
        WHEN FORMAT_DATE('%G-%V', CAST(DATETIME(t.start_at, COALESCE(p.timezone, ct.timezone)) AS DATE)) = FORMAT_DATE('%G-%V', DATE_SUB('{{ next_ds }}', INTERVAL 1 WEEK))
          THEN '1_week_ago'
        ELSE FORMAT_DATE('%G-%V', CAST(DATETIME(t.start_at, COALESCE(p.timezone, ct.timezone)) AS DATE))
      END AS week_relative
  FROM `{{ params.project_id }}.cl.porygon_events` p
  LEFT JOIN UNNEST(p.transactions) t
  LEFT JOIN `{{ params.project_id }}.cl.countries` co ON p.country_code = co.country_code
  LEFT JOIN UNNEST(co.cities) ci ON p.country_code = co.country_code
    AND p.city_id = ci.id
  LEFT JOIN UNNEST(ci.zones) z ON p.country_code = co.country_code
    AND t.zone_id = z.id
  LEFT JOIN country_timezones ct ON p.country_code = ct.country_code
  WHERE t.is_active IS TRUE
    AND p.created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 2 MONTH)
)
SELECT e.country_code
  , e.country_name
  , e.city_id
  , e.city_name
  , e.zone_id
  , e.zone_name
  , e.platform
  , r.report_date
  , r.start_datetime
  , r.end_datetime
  , e.event_duration
  , CASE
      WHEN e.starts_at_local <= r.start_datetime
        AND e.ends_at_local <= r.end_datetime
        THEN TIMESTAMP_DIFF(e.ends_at_local, r.start_datetime, SECOND) / 60
      WHEN e.starts_at_local <= r.start_datetime
        AND e.ends_at_local >= r.end_datetime
        THEN TIMESTAMP_DIFF(r.end_datetime, r.start_datetime, SECOND) / 60
      WHEN e.starts_at_local >= r.start_datetime
        AND e.ends_at_local <= r.end_datetime
        THEN TIMESTAMP_DIFF(e.ends_at_local, e.starts_at_local, SECOND) / 60
      WHEN e.starts_at_local >= r.start_datetime
        AND e.ends_at_local >= r.end_datetime
        THEN TIMESTAMP_DIFF(r.end_datetime, e.starts_at_local, SECOND) / 60
      ELSE 0
    END AS event_split_duration
  , TIMESTAMP_TRUNC(e.starts_at_local, SECOND) AS starts_at_local
  , TIMESTAMP_TRUNC(e.ends_at_local, SECOND) AS ends_at_local
  , e.event_id
  , e.event_title
  , e.halal
  , e.value
  , e.action
  , e.week_relative
FROM report_dates r
LEFT JOIN events e ON r.report_date = e.report_date
  AND e.ends_at_local >= r.start_datetime ------------- Take all events belonging the interval, even only in part
  AND e.starts_at_local < r.end_datetime
-- removing darkstore pickers to not impact utr as they do not have orders, only working hours
WHERE e.country_code NOT LIKE '%dp%'
;
