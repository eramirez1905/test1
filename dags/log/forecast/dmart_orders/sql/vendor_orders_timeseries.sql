CREATE TEMPORARY FUNCTION floor_timestamp(x TIMESTAMP) AS
(
  TIMESTAMP_ADD(
    TIMESTAMP_TRUNC(x, HOUR),
    INTERVAL CAST(FLOOR(EXTRACT(MINUTE FROM x) / 30) * 30 AS INT64) MINUTE
  )
);

CREATE OR REPLACE TABLE dmart_order_forecast.vendor_orders_timeseries
PARTITION BY created_date
CLUSTER BY country_code, vendor_code 
AS
WITH orders AS
(
  SELECT
    o.country_code,
    o.order_id,
    o.vendor.vendor_code AS vendor_code,
    o.order_placed_at,
    o.promised_delivery_time AS order_expected_at,
    'corporate' IN UNNEST(o.tags) AS tag_corporate,
    'halal' IN UNNEST(o.tags) AS tag_halal,
    'preorder' IN UNNEST(o.tags) AS tag_preorder,
    o.cancellation.source AS cancelled_by,
    o.order_status AS status
  FROM `fulfillment-dwh-production.cl.orders` o
  WHERE 
    # TODO: to save money
    o.created_date >= '2020-01-01'
),
orders_timeseries AS
(
  SELECT
    o.country_code,
    o.vendor_code,
    floor_timestamp(
      CASE
        WHEN o.tag_preorder THEN TIMESTAMP_SUB(o.order_expected_at, INTERVAL 25 MINUTE)
        ELSE TIMESTAMP_ADD(o.order_placed_at, INTERVAL 10 MINUTE)
      END
    ) AS datetime,
    COUNTIF(o.status = 'completed' OR (o.status != 'completed' AND o.cancelled_by = 'dispatcher')) AS orders,
    COUNT(DISTINCT o.order_id) AS orders_all,
    COUNTIF(o.status = 'completed') AS orders_completed,
    COUNTIF(o.status = 'cancelled') AS orders_cancelled,
    COUNTIF(o.cancelled_by = 'system') AS orders_system_cancelled,
    COUNTIF(o.cancelled_by = 'dispatcher') AS orders_dispatcher_cancelled,
    COUNTIF(o.tag_halal) AS tag_halal,
    COUNTIF(o.tag_preorder) AS tag_preorder,
    COUNTIF(o.tag_corporate) AS tag_corporate
  FROM orders o
  GROUP BY 1,2,3
)
SELECT
  *,
  DATE(datetime) AS created_date
FROM orders_timeseries
WHERE 
  datetime <= '{{next_execution_date}}'
