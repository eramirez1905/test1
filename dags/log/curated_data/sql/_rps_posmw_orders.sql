CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._rps_posmw_orders`
PARTITION BY created_date
CLUSTER BY region, order_id AS
WITH posmw_last_order_status_dataset AS (
  -- temporary solution due to duplicated data coming from product
  -- issue: pos orders with more than one final status
  SELECT region
    , order_id
    , MIN(created_date) AS created_date
  FROM `{{ params.project_id }}.dl.posmw_last_order_status`
  GROUP BY 1,2
), powmw_orders AS (
  SELECT region
    , order_id
    , 'POS' AS client_name
    , created_date
    , LOWER(order_id) AS _order_id
  FROM posmw_last_order_status_dataset
), posmw_order_id_mapped_dataset AS (
  -- temporary solution due to duplicated data coming from product
  -- issue: orders with more than one pos order id
  SELECT *
  FROM (
    SELECT region
      , LOWER(order_id) AS _order_id
      , pos_order_id
      , ROW_NUMBER() OVER (PARTITION BY region, LOWER(order_id) ORDER BY created_at DESC) AS _row_number
    FROM `{{ params.project_id }}.dl.posmw_order_id_mapped`
  )
  WHERE _row_number = 1
), posmw_order_id_mapped AS (
  SELECT region
    , _order_id
    , pos_order_id
  FROM posmw_order_id_mapped_dataset
)
SELECT o.region
  , o.created_date
  , o.order_id
  , o.client_name
  , om.pos_order_id
FROM powmw_orders o
LEFT JOIN posmw_order_id_mapped om ON o._order_id = om._order_id
  AND o.region = om.region
