CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._order_transitions`
PARTITION BY created_date
CLUSTER BY country_code, order_id, to_state AS
SELECT country_code
  , CAST(created_at AS DATE) AS created_date
  , order_id
  , to_state
  , most_recent
  , created_at
  , CAST(JSON_EXTRACT_SCALAR(metadata, '$.user_id') AS INT64) AS user_id
  , CAST(JSON_EXTRACT_SCALAR(metadata, '$.performed_by') AS STRING) AS performed_by
  , CAST(COALESCE(JSON_EXTRACT_SCALAR(metadata, '$.cancellation_metadata.reason'), JSON_EXTRACT_SCALAR(metadata, '$.reason')) AS STRING) AS reason
FROM `{{ params.project_id }}.dl.hurrier_order_transitions`
