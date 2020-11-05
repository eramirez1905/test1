CREATE OR REPLACE TABLE rl.scorecard_vendor_compliance_weights
PARTITION BY report_date_local AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 6 MONTH) AS start_time
    , DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY) AS end_time
), orders AS (
  SELECT o.country_code
    , o.city_id
    , COALESCE(CAST(DATETIME(d.rider_dropped_off_at, o.timezone) AS DATE), CAST(DATETIME(o.created_at, o.timezone) AS DATE)) AS report_date_local
    , COUNT(DISTINCT(IF(order_status = 'cancelled', order_id, NULL))) AS cancelled_orders_total
    , COUNT(DISTINCT(IF(order_status = 'cancelled' AND o.cancellation.source = 'dispatcher', order_id, NULL))) AS cancelled_orders
    , COUNT(DISTINCT(IF(order_status = 'cancelled' AND o.cancellation.source = 'dispatcher' AND o.cancellation.reason = 'TRAFFIC_MANAGER_VENDOR_CLOSED', order_id, NULL))) AS vendor_cancellations
    , AVG(IF(d.delivery_status = 'completed', o.estimated_prep_time / 60, NULL)) AS estimated_prep_duration
    , COUNT(DISTINCT(IF(d.delivery_status = 'completed' AND o.timings.vendor_late / 60 >  5, o.order_id, NULL))) AS orders_late
    , COUNT(DISTINCT(IF(d.delivery_status = 'completed', order_id, NULL))) AS completed_orders
  FROM cl.orders o
  LEFT JOIN UNNEST(deliveries) d
  WHERE o.created_date >= (SELECT start_time FROM parameters)
    AND COALESCE(CAST(DATETIME(d.rider_dropped_off_at, o.timezone) AS DATE), CAST(DATETIME(o.created_at, o.timezone) AS DATE)) BETWEEN (SELECT start_time FROM parameters) AND (SELECT end_time FROM parameters)
  GROUP BY 1, 2, 3
------------------------------ UTR --------------------------------
), utr AS (
  SELECT country_code
    , d.city_id
    , COALESCE(DATE(DATETIME(d.rider_dropped_off_at, o.timezone)), DATE(DATETIME(d.created_at, o.timezone))) AS report_date
    , IF(d.delivery_status = 'completed', o.order_id, NULL) AS order_completed
    , NULL AS working_hours
  FROM cl.orders o
  LEFT JOIN UNNEST(deliveries) d
  WHERE o.created_date >= (SELECT start_time FROM parameters)

  UNION ALL

  SELECT country_code
    , city_id
    , e.day AS report_date
    , NULL AS order_completed
    , (e.duration / 3600) AS working_hours
  FROM cl.shifts s
  LEFT JOIN UNNEST(s.actual_working_time_by_date) e
  WHERE s.created_date >= (SELECT start_time FROM parameters)
), utr_agg AS (
  SELECT country_code
    , city_id
    , report_date AS report_date_local
    , COUNT(DISTINCT order_completed) AS orders_completed
    , SUM(working_hours) AS working_hours
  FROM utr
  GROUP BY 1, 2, 3
)
-------------------------------- FINAL AGGREGATION --------------------------------
SELECT u.country_code
  , u.city_id
  , u.report_date_local
  , SAFE_DIVIDE(orders_completed, working_hours) AS utr
  , COALESCE(SAFE_DIVIDE(vendor_cancellations, cancelled_orders), 0) AS cancellation_rate
  , (1 - SAFE_DIVIDE(orders_late, completed_orders)) AS reliability_rate
  , estimated_prep_duration
FROM utr_agg u
LEFT JOIN orders o ON u.country_code = o.country_code
  AND u.city_id = o.city_id
  AND u.report_date_local = o.report_date_local
;
