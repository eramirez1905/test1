WITH dag_executions_cl AS (
  SELECT *
    , ARRAY_AGG(
        STRUCT(
          dataset
          , next_execution_date
          , execution_date
        )
      ) OVER (ORDER BY next_execution_date ASC ROWS BETWEEN 300 PRECEDING AND CURRENT ROW) AS raw
  FROM `fulfillment-dwh-production.cl.dag_executions`
), dag_executions AS (
  SELECT execution_date
    , next_execution_date
    , ANY_VALUE((SELECT MAX(next_execution_date) FROM d.raw WHERE dataset = 'raw')) AS max_raw
  FROM dag_executions_cl d
  WHERE dag_id LIKE 'curated-data-v%' AND is_shared
  GROUP BY 1,2
)
SELECT table_name
  , MAX(_ingested_at) AS last_updated
  , MAX(d.next_execution_date) AS execution_date
  , MAX(max_raw) AS max_raw
FROM `fulfillment-dwh-production.cl.dag_executions`
CROSS JOIN dag_executions d
WHERE dag_id LIKE 'curated-data-v%' AND is_shared
GROUP BY 1
