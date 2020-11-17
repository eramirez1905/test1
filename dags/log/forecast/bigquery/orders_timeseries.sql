CREATE TEMPORARY FUNCTION floor_timestamp(x TIMESTAMP) AS 
(
  DATETIME_ADD(
    DATETIME_TRUNC(CAST(x AS DATETIME), HOUR), 
    INTERVAL CAST(FLOOR(EXTRACT(MINUTE FROM CAST(x AS DATETIME)) / 30) * 30 AS INT64) MINUTE
    )
);

CREATE OR REPLACE TABLE forecasting.orders_timeseries
PARTITION BY created_date
CLUSTER BY country_code AS
WITH orders AS
(
  SELECT
    o.country_code,
    o.order_id,
    o.zone_id,
    o.order_placed_at,
    o.promised_delivery_time AS order_expected_at,
    'corporate' IN UNNEST(o.tags) AS tag_corporate,
    'halal' IN UNNEST(o.tags) AS tag_halal,
    'preorder' IN UNNEST(o.tags) AS tag_preorder,
    o.order_status AS status,
    CASE 
      WHEN o.cancellation.source = 'api' THEN 'system'
      WHEN o.cancellation.source = 'dispatcher' THEN 'dispatcher'
    END AS cancelled_by
  FROM `fulfillment-dwh-production.cl.orders` o
  WHERE 
    -- approx. date of first hurrier migration
    o.order_placed_at >= '2015-12-01 00:00:00'
  ),
  orders_timeseries AS
  (
  SELECT 
    o.country_code,
    -- map all orders without zones to zone_id 0 to keep visibility
    COALESCE(o.zone_id, 0) AS zone_id,
    CAST(floor_timestamp(
      CASE 
        WHEN o.tag_preorder THEN TIMESTAMP_SUB(o.order_expected_at, INTERVAL 25 MINUTE)
        ELSE TIMESTAMP_ADD(o.order_placed_at, INTERVAL 10 MINUTE)
      END
      ) AS TIMESTAMP) AS datetime,
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
  -- for idempotency
  datetime <= '{{next_execution_date}}'
