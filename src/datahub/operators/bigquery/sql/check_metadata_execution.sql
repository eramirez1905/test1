WITH execution_date AS (
  SELECT next_execution_date
  FROM `{{ params.project_id }}.{{ params.metadata_dataset }}.{{params.metadata_table_name}}`
  WHERE DATE(execution_date) BETWEEN DATE_SUB('{{ next_ds }}', INTERVAL 2 DAY) AND '{{ next_ds }}'
   AND dataset = '{{ params.dataset }}'
   AND table_name = '{{ params.table_name }}'
  ORDER BY next_execution_date DESC
  LIMIT 1
)
SELECT next_execution_date > '{{ execution_date }}'
FROM execution_date
