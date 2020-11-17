WITH max_created_timestamp AS (
  SELECT region
    , MAX(created_at) AS max_created_at
  FROM `{{ params.project_id }}.cl.rps_orders`
  GROUP BY 1
), orders AS (
  SELECT o.region
    , mct.max_created_at
    , o.order_id
    , COUNT(*) AS count
    , COUNTIF(o.entity.id IS NULL) AS null_entity_id_count
    , TIMESTAMP_DIFF('{{ next_execution_date }}', mct.max_created_at, HOUR) AS max_created_at_diff
  FROM `{{ params.project_id }}.cl.rps_orders` o
  LEFT JOIN max_created_timestamp mct ON o.region = mct.region
  GROUP BY 1,2,3
)
SELECT COUNTIF(count != 1) = 0 AS duplicated_orders_check
  , COUNTIF(null_entity_id_count > 0) = 0 AS null_entity_id_check
  , COUNTIF(max_created_at_diff > 24) = 0 AS timeliness_check
FROM orders
