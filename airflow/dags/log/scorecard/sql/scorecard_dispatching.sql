CREATE OR REPLACE TABLE rl.scorecard_dispatching AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 10 WEEK) AS start_time
    , DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY) AS end_time
), issues AS (
  SELECT i.country_code
    , i.city_id
    , FORMAT_DATE('%G-%V', CAST(DATETIME(i.created_at, i.timezone) AS DATE)) AS report_week_local
    , SUM(TIMESTAMP_DIFF(i.resolved_at, i.created_at, MINUTE)) AS time_to_resolve_sum
    , COUNT(DISTINCT i.issue_id) AS issues
  FROM cl.issues i
  WHERE i.issue_category IN ('waiting', 'order_clicked_through', 'no_courier_interaction', 'no_gps_and_delayed')
    AND i.created_date BETWEEN (SELECT start_time FROM parameters) AND (SELECT end_time FROM parameters)
  GROUP BY 1, 2, 3
), manual_actions AS (
  SELECT a.country_code
    , FORMAT_DATE('%G-%V', COALESCE(CAST(DATETIME(d.rider_dropped_off_at, o.timezone) AS DATE), CAST(DATETIME(o.created_at, o.timezone) AS DATE))) AS report_week_local
    , o.city_id
    , COUNT(DISTINCT(IF(a.action IN ('cancel_order', 'update_order', 'send_to_vendor', 'manual_dispatch', 'replace_delivery', 'manual_undispatch', 'delivery_time_updated', 'update_courier_route', 'update_delivery_status'), o.order_id, NULL))) AS manually_touched_orders
    , COUNT(DISTINCT order_id) AS all_orders
  FROM cl.orders o
  LEFT JOIN UNNEST(deliveries) d
  LEFT JOIN cl.audit_logs a ON o.country_code = a.country_code
    AND o.platform_order_code = hurrier.order_code
    AND a.created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 10 WEEK)
  WHERE o.created_date >= (SELECT start_time FROM parameters)
    AND COALESCE(CAST(DATETIME(d.rider_dropped_off_at, o.timezone) AS DATE), CAST(DATETIME(o.created_at, o.timezone) AS DATE)) BETWEEN (SELECT start_time FROM parameters) AND (SELECT end_time FROM parameters)
  GROUP BY 1, 2, 3
)
-------------------------------- FINAL AGGREGATION --------------------------------
SELECT a.country_code
  , a.city_id
  , a.report_week_local
  , SAFE_DIVIDE(time_to_resolve_sum, issues) AS avg_issue_solving_time
  , SAFE_DIVIDE(a.manually_touched_orders, a.all_orders) AS perc_manually_touched_orders
FROM  manual_actions a
LEFT JOIN issues i ON a.country_code = i.country_code
  AND a.city_id = i.city_id
  AND a.report_week_local = i.report_week_local
;
