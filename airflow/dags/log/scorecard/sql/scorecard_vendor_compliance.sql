CREATE OR REPLACE TABLE rl.scorecard_vendor_compliance AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 10 WEEK) AS start_time
    , DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY) AS end_time
), orders AS (
  SELECT o.country_code
    , o.city_id
    , FORMAT_DATE('%G-%V', COALESCE(CAST(DATETIME(d.rider_dropped_off_at, o.timezone) AS DATE), CAST(DATETIME(o.created_at, o.timezone) AS DATE))) AS report_week_local
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
)
  SELECT country_code
    , city_id
    , report_week_local
    , COALESCE(SAFE_DIVIDE(vendor_cancellations, cancelled_orders), 0) AS cancellation_rate
    , (1 - SAFE_DIVIDE(orders_late, completed_orders)) AS reliability_rate
    , estimated_prep_duration
  FROM orders
;
