CREATE OR REPLACE TABLE rl.scorecard_dispatching_weights
PARTITION BY report_date_local AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 6 MONTH) AS start_time
    , DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY) AS end_time
), issues AS (
  SELECT i.country_code
    , i.city_id
    , CAST(DATETIME(i.created_at, i.timezone) AS DATE) AS report_date_local
    , SUM(TIMESTAMP_DIFF(i.resolved_at, i.created_at, MINUTE)) AS time_to_resolve_sum
    , COUNT(DISTINCT i.issue_id) AS issues
  FROM cl.issues i
  WHERE i.issue_category IN ('waiting', 'order_clicked_through', 'no_courier_interaction', 'no_gps_and_delayed')
    AND i.created_date BETWEEN (SELECT start_time FROM parameters) AND (SELECT end_time FROM parameters)
  GROUP BY 1, 2, 3
), manual_actions AS(
  SELECT a.country_code
    , COALESCE(CAST(DATETIME(d.rider_dropped_off_at, o.timezone) AS DATE), CAST(DATETIME(o.created_at, o.timezone) AS DATE)) AS report_date_local
    , o.city_id
    , COUNT(DISTINCT o.order_id) AS manually_touched_orders
  FROM cl.orders o
  LEFT JOIN UNNEST(deliveries) d
  LEFT JOIN cl.audit_logs a ON o.country_code = a.country_code
    AND o.platform_order_code = hurrier.order_code
    AND a.created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 10 WEEK)
  WHERE o.created_date >= (SELECT start_time FROM parameters)
    AND COALESCE(CAST(DATETIME(d.rider_dropped_off_at, o.timezone) AS DATE), CAST(DATETIME(o.created_at, o.timezone) AS DATE)) BETWEEN (SELECT start_time FROM parameters) AND (SELECT end_time FROM parameters)
    AND a.action IN ('cancel_order', 'update_order', 'send_to_vendor', 'manual_dispatch', 'replace_delivery', 'manual_undispatch', 'delivery_time_updated', 'update_courier_route', 'update_delivery_status')
  GROUP BY 1, 2, 3
------------------------------ UTR --------------------------------
), utr AS (
  SELECT country_code
    , d.city_id
    , COALESCE(DATE(DATETIME(d.rider_dropped_off_at, o.timezone)), DATE(DATETIME(d.created_at, o.timezone))) AS report_date
    , o.order_id
    , d.delivery_status
    , NULL AS working_hours
  FROM cl.orders o
  LEFT JOIN UNNEST(deliveries) d
  WHERE o.created_date >= (SELECT start_time FROM parameters)

  UNION ALL

  SELECT country_code
    , city_id
    , e.day AS report_date
    , NULL AS order_id
    , NULL AS delivery_status
    , (e.duration / 3600) AS working_hours
  FROM cl.shifts s
  LEFT JOIN UNNEST(s.actual_working_time_by_date) e
  WHERE s.created_date >= (SELECT start_time FROM parameters)
), utr_agg AS (
  SELECT country_code
    , city_id
    , report_date AS report_date_local
    , COUNT(DISTINCT(IF(delivery_status = 'completed', order_id, NULL))) AS orders_completed
    , COUNT(DISTINCT order_id) AS all_orders
    , SUM(working_hours) AS working_hours
  FROM utr
  GROUP BY 1, 2, 3
)
-------------------------------- FINAL AGGREGATION --------------------------------
SELECT u.country_code
  , u.city_id
  , u.report_date_local
  , SAFE_DIVIDE(orders_completed, working_hours) AS utr
  , SAFE_DIVIDE(time_to_resolve_sum, issues) AS avg_issue_solving_time
  , SAFE_DIVIDE(a.manually_touched_orders, u.all_orders) AS perc_manually_touched_orders
FROM utr_agg u
LEFT JOIN manual_actions a ON u.country_code = a.country_code
  AND u.city_id = a.city_id
  AND u.report_date_local = a.report_date_local
LEFT JOIN issues i ON u.country_code = i.country_code
  AND u.city_id = i.city_id
  AND u.report_date_local = i.report_date_local
;
