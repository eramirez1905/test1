CREATE OR REPLACE TABLE rl.scorecard_dispatching_zones AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 10 WEEK) AS start_time
    , DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY) AS end_time
), deliveries_to_zones AS (
  SELECT o.country_code
    , d.city_id
    , o.zone_id
    , FORMAT_DATE('%G-%V', COALESCE(CAST(DATETIME(d.rider_dropped_off_at, o.timezone) AS DATE), CAST(DATETIME(o.created_at, o.timezone) AS DATE))) AS report_week_local
    , COALESCE(CAST(DATETIME(d.rider_dropped_off_at, o.timezone) AS DATE), CAST(DATETIME(o.created_at, o.timezone) AS DATE)) AS report_date_local
    , o.order_id
    , o.platform_order_code AS order_code
    , d.id AS delivery_id
  FROM cl.orders o
  LEFT JOIN UNNEST(deliveries) d
  WHERE o.created_date >= (SELECT start_time FROM parameters)
), issues AS (
SELECT i.country_code
    , i.city_id
    , dz.zone_id
    , FORMAT_DATE('%G-%V', CAST(DATETIME(i.created_at, i.timezone) AS DATE)) AS report_week_local
    , SUM(TIMESTAMP_DIFF(i.resolved_at, i.created_at, MINUTE)) AS time_to_resolve_sum
    , COUNT (DISTINCT i.issue_id) AS issues
  FROM cl.issues i
  LEFT JOIN deliveries_to_zones dz ON i.country_code = dz.country_code
    AND i.delivery_id = dz.delivery_id
  WHERE i.issue_category IN ('waiting', 'order_clicked_through', 'no_courier_interaction', 'no_gps_and_delayed')
    AND i.created_date BETWEEN (SELECT start_time FROM parameters) AND (SELECT end_time FROM parameters)
  GROUP BY 1, 2, 3, 4
  ORDER BY 1, 2, 3
), manual_actions AS (
  SELECT d.country_code
    , d.report_week_local
    , d.city_id
    , d.zone_id
    , COUNT(DISTINCT(IF(a.action IN ('cancel_order', 'update_order', 'send_to_vendor', 'manual_dispatch', 'replace_delivery', 'manual_undispatch', 'delivery_time_updated', 'update_courier_route', 'update_delivery_status'), d.order_id, NULL))) AS manually_touched_orders
    , COUNT(DISTINCT order_id) AS all_orders
  FROM deliveries_to_zones d
  LEFT JOIN cl.audit_logs a ON d.country_code = a.country_code
    AND d.order_code = hurrier.order_code
    AND a.created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 10 WEEK)
  WHERE d.report_date_local BETWEEN (SELECT start_time FROM parameters) AND (SELECT end_time FROM parameters)
  GROUP BY 1, 2, 3, 4
)
-------------------------------- FINAL AGGREGATION --------------------------------
SELECT d.country_code
  , d.city_id
  , d.zone_id
  , d.report_week_local
  , SAFE_DIVIDE(time_to_resolve_sum, issues) AS avg_issue_solving_time
  , SAFE_DIVIDE(manually_touched_orders, all_orders) AS perc_manually_touched_orders
FROM manual_actions d
LEFT JOIN issues i ON d.country_code = i.country_code
  AND d.city_id = i.city_id
  AND d.zone_id = i.zone_id
  AND d.report_week_local = i.report_week_local
;
