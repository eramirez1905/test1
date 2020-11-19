CREATE OR REPLACE TABLE il.sr_lost_orders AS
WITH parameters AS (
  SELECT DATE_SUB(DATE_SUB('{{ next_ds }}', INTERVAL 5 WEEK), INTERVAL 1 DAY) AS start_date
    , DATE_ADD('{{ next_ds }}', INTERVAL 2 WEEK) AS end_date
), dates AS (
  SELECT date_time
  FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(
    TIMESTAMP_TRUNC(TIMESTAMP_SUB((SELECT CAST(end_date AS TIMESTAMP) FROM parameters), INTERVAL 50 DAY), DAY),
    TIMESTAMP_TRUNC((SELECT CAST(end_date AS TIMESTAMP) FROM parameters), HOUR), INTERVAL 15 MINUTE)
  ) AS date_time
), countries AS (
  SELECT country_code
    , ci.id AS city_id
    , ci.timezone AS timezone
    , zo.id AS zone_id
  FROM `{{ params.project_id }}.cl.countries` co
  LEFT JOIN UNNEST(co.cities) ci
  LEFT JOIN UNNEST(ci.zones) zo
), report_dates AS (
  SELECT CAST(date_time AS DATE) AS report_date
    , date_time AS start_datetime
    , TIMESTAMP_ADD(date_time, INTERVAL 15 MINUTE) AS end_datetime
    , LAG(CAST(date_time AS DATE),21) OVER(ORDER BY CAST(date_time AS DATE) ASC) AS working_day
  FROM dates
), grouped_close_orders AS (
  SELECT lo.country_code
    , lo.zone_id
    , lo.datetime
    , z.timezone
    , MIN(TIMESTAMP_SECONDS(900 * DIV(UNIX_SECONDS(lo.starts_at) + 0, 900))) AS min_starts_at
    , MAX(TIMESTAMP_SECONDS(900 * DIV(UNIX_SECONDS(lo.ends_at) + 840, 900))) AS max_ends_at
    , ROUND(SUM(lo.orders_extrapolated)) AS orders_lost_estimate
    , SUM(lo.orders_lost_net) AS orders_lost_net
  FROM lost_orders.orders_lost_extrapolated lo
  LEFT JOIN countries z ON z.country_code = lo.country_code
    AND z.zone_id = lo.zone_id
  WHERE CAST(lo.datetime AS DATE) BETWEEN (SELECT start_date FROM parameters) AND (SELECT end_date FROM parameters)
    AND lo.event_type = 'close'
  GROUP BY
    country_code
    , zone_id
    , datetime
    , timezone
), split_close_orders AS (
  SELECT country_code
    , zone_id
    , min_starts_at AS datetime
    , orders_lost_estimate / 2 AS orders_lost_estimate
    , orders_lost_net / 2 AS orders_lost_net
    , timezone
  FROM grouped_close_orders
  WHERE TIMESTAMP_DIFF(max_ends_at, min_starts_at, MINUTE) > 15
  UNION ALL
  SELECT country_code
    , zone_id
    , TIMESTAMP_ADD(min_starts_at, INTERVAL 15 MINUTE) AS datetime
    , orders_lost_estimate / 2 AS orders_lost_estimate
    , orders_lost_net / 2 AS orders_lost_net
    , timezone
  FROM grouped_close_orders
  WHERE TIMESTAMP_DIFF(max_ends_at, min_starts_at, MINUTE) > 15
  UNION ALL
  SELECT country_code
    , zone_id
    , min_starts_at AS datetime
    , orders_lost_estimate
    , orders_lost_net
    , timezone
  FROM grouped_close_orders
  WHERE TIMESTAMP_DIFF(max_ends_at, min_starts_at, MINUTE) = 15
), grouped_shrink_orders AS (
  SELECT lo.country_code
    , lo.zone_id
    , lo.datetime
    , z.timezone
    , MIN(TIMESTAMP_SECONDS(900 * DIV(UNIX_SECONDS(lo.starts_at) + 0, 900))) AS min_starts_at
    , MAX(TIMESTAMP_SECONDS(900 * DIV(UNIX_SECONDS(lo.ends_at) + 840, 900))) AS max_ends_at
    , ROUND(SUM(lo.orders_extrapolated)) AS orders_lost_estimate
    , SUM(lo.orders_lost_net) AS orders_lost_net
  FROM lost_orders.orders_lost_extrapolated lo
  LEFT JOIN countries z ON z.country_code = lo.country_code
    AND z.zone_id = lo.zone_id
  WHERE CAST(lo.datetime AS DATE) BETWEEN (SELECT start_date FROM parameters) AND (SELECT end_date FROM parameters)
    AND lo.event_type IN ('shrink', 'shrink_legacy')
  GROUP BY
    country_code
    , zone_id
    , datetime
    , timezone
), split_shrink_orders AS (
  SELECT country_code
    , zone_id
    , min_starts_at AS datetime
    , orders_lost_estimate / 2 AS orders_lost_estimate
    , orders_lost_net / 2 AS orders_lost_net
    , timezone
  FROM grouped_shrink_orders
  WHERE TIMESTAMP_DIFF(max_ends_at, min_starts_at, MINUTE) > 15
  UNION ALL
  SELECT country_code
    , zone_id
    , TIMESTAMP_ADD(min_starts_at, INTERVAL 15 MINUTE) AS datetime
    , orders_lost_estimate / 2 AS orders_lost_estimate
    , orders_lost_net / 2 AS orders_lost_net
    , timezone
  FROM grouped_shrink_orders
  WHERE TIMESTAMP_DIFF(max_ends_at, min_starts_at, MINUTE) > 15
  UNION ALL
  SELECT country_code
    , zone_id
    , min_starts_at AS datetime
    , orders_lost_estimate
    , orders_lost_net
    , timezone
  FROM grouped_shrink_orders
  WHERE TIMESTAMP_DIFF(max_ends_at, min_starts_at, MINUTE) = 15
), all_lost_orders AS (
  SELECT COALESCE(c.country_code, s.country_code) AS country_code
    , COALESCE(c.zone_id, s.zone_id) AS zone_id
    , COALESCE(c.datetime, s.datetime) AS datetime
    , COALESCE(c.orders_lost_estimate, 0) AS close_orders_lost_estimate
    , COALESCE(c.orders_lost_net, 0) AS close_orders_lost_net
    , COALESCE(s.orders_lost_estimate, 0) AS shrink_orders_lost_estimate
    , COALESCE(s.orders_lost_net, 0) AS shrink_orders_lost_net
    , COALESCE(c.timezone, s.timezone) AS timezone
  FROM split_close_orders AS c
  FULL OUTER JOIN split_shrink_orders AS s ON c.country_code = s.country_code
    AND c.zone_id = s.zone_id
    AND c.datetime = s.datetime
)
SELECT lo.country_code
  , z.city_id
  , lo.zone_id
  , CAST(DATETIME(dates.start_datetime, lo.timezone) AS DATE) AS report_date_local
  , DATETIME(dates.start_datetime, lo.timezone) AS start_datetime_local
  , DATETIME(dates.end_datetime, lo.timezone) AS end_datetime_local
  , close_orders_lost_estimate
  , close_orders_lost_net
  , shrink_orders_lost_estimate
  , shrink_orders_lost_net
  , z.timezone
FROM all_lost_orders lo
INNER JOIN report_dates dates ON dates.start_datetime = lo.datetime
LEFT JOIN countries z ON z.country_code = lo.country_code
  AND z.zone_id = lo.zone_id
;
