CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.closure_report_lost_orders`
PARTITION BY DATE(start_datetime) AS
WITH dates AS (
  SELECT date
  FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 60 DAY), DAY), TIMESTAMP_TRUNC(CURRENT_TIMESTAMP, HOUR), INTERVAL 30 MINUTE)) AS date
), report_dates AS (
  SELECT CAST(date AS DATE) AS report_date
    , date AS start_datetime
    , TIMESTAMP_ADD(date, INTERVAL 30 MINUTE) AS end_datetime
  FROM dates
), lost_orders_close AS (
  SELECT lo.country_code
    , zone_id
    , report_date
    , start_datetime
    , end_datetime
    , SUM(orders_lost_net) AS orders_lost_net
    , SUM(orders_extrapolated) AS orders_lost_estimate
  FROM report_dates d
  INNER JOIN lost_orders.orders_lost_extrapolated lo ON d.report_date = DATE(lo.datetime)
    AND lo.starts_at >= d.start_datetime
    AND lo.starts_at < d.end_datetime
    AND lo.event_type = 'close'
  GROUP BY 1, 2, 3, 4, 5
), lost_orders_shrink AS (
  SELECT lo.country_code
    , zone_id
    , report_date
    , start_datetime
    , end_datetime
    , SUM(orders_lost_net) AS orders_lost_net
    , SUM(orders_extrapolated) AS orders_lost_estimate
  FROM report_dates d
  INNER JOIN lost_orders.orders_lost_extrapolated lo ON d.report_date = DATE(lo.datetime)
    AND lo.starts_at >= d.start_datetime
    AND lo.starts_at < d.end_datetime
    AND lo.event_type IN ('shrink', 'shrink_legacy')
  GROUP BY 1, 2, 3, 4, 5
), orders AS (
  SELECT d.report_date
    , d.start_datetime
    , d.end_datetime
    , o.country_code
    , o.city_id
    , o.timezone
    , o.zone_id
    , COUNT(DISTINCT o.order_id) AS completed_orders
    , AVG(IF(o.is_preorder IS FALSE, o.timings.actual_delivery_time / 60, NULL)) AS delivery_time
    , AVG((SELECT od.timings.delivery_delay FROM UNNEST(o.deliveries) od LIMIT 1) / 60) AS delay
  FROM `{{ params.project_id }}.cl.orders` o
  LEFT JOIN report_dates d ON o.created_date = d.report_date
    AND o.created_at >= d.start_datetime
    AND o.created_at < d.end_datetime
  LEFT JOIN UNNEST(o.deliveries) del
  WHERE del.delivery_status = 'completed'
  GROUP BY 1, 2, 3, 4, 5, 6, 7
)
SELECT o.country_code
  , co.country_name
  , o.city_id
  , ci.name AS city_name
  , o.zone_id
  , zo.name AS zone_name
  , CAST(DATETIME(o.start_datetime, o.timezone) AS DATE) AS report_date
  , DATETIME(o.start_datetime, o.timezone) AS start_time_local
  , DATETIME(o.end_datetime, o.timezone) AS end_time_local
  , o.start_datetime
  , o.completed_orders
  , o.delivery_time
  , o.delay
  , IF(loc.orders_lost_net IS NOT NULL, loc.orders_lost_net, 0) AS orders_lost_net_close
  , IF(loc.orders_lost_estimate IS NOT NULL, loc.orders_lost_estimate, 0) AS orders_lost_estimate_close
  , IF(los.orders_lost_net IS NOT NULL, los.orders_lost_net, 0) AS orders_lost_net_shrink
  , IF(los.orders_lost_estimate IS NOT NULL, los.orders_lost_estimate, 0) AS orders_lost_estimate_shrink
  , CASE
      WHEN FORMAT_DATE('%G-%V', CAST(DATETIME(o.start_datetime, o.timezone) AS DATE)) = FORMAT_DATE('%G-%V', '{{ next_ds }}')
        THEN 'current_week'
      WHEN FORMAT_DATE('%G-%V', CAST(DATETIME(o.start_datetime, o.timezone) AS DATE)) = FORMAT_DATE('%G-%V', DATE_SUB('{{ next_ds }}', INTERVAL 1 WEEK))
        THEN '1_week_ago'
      ELSE FORMAT_DATE('%G-%V', CAST(DATETIME(o.start_datetime, o.timezone) AS DATE))
    END AS week_relative
FROM orders o
LEFT JOIN lost_orders_close loc ON loc.country_code = o.country_code
  AND loc.zone_id = o.zone_id
  AND loc.start_datetime = o.start_datetime
  AND loc.end_datetime = o.end_datetime
LEFT JOIN lost_orders_shrink los ON los.country_code = o.country_code
  AND los.zone_id = o.zone_id
  AND los.start_datetime = o.start_datetime
  AND los.end_datetime = o.end_datetime
LEFT JOIN `{{ params.project_id }}.cl.countries` co ON o.country_code = co.country_code
LEFT JOIN UNNEST(co.cities) ci ON o.city_id = ci.id
LEFT JOIN UNNEST(ci.zones) zo ON o.zone_id = zo.id
-- removing darkstore pickers to not impact utr as they do not have orders, only working hours
WHERE o.country_code NOT LIKE '%dp%'
