CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.rps_gsr_daily_dashboard_orders_rps_share`
PARTITION BY created_date
CLUSTER BY entity_id AS
WITH rps_orders_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl.rps_orders`
  WHERE created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 18 MONTH)
)
SELECT CAST(DATETIME(created_at, timezone) AS DATE) AS created_date
  , entity.id AS entity_id
  , COUNT(DISTINCT IF(NOT order_status = 'completed', order_id, NULL)) AS no_fail_orders
  , COUNT(DISTINCT order_id) AS no_orders
FROM rps_orders_dataset
GROUP BY 1,2
