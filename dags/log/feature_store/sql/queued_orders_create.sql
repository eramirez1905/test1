CREATE TABLE `{{ params.project_id }}.{{ params.feature_store_dataset }}.{{ params.table_name }}`
(
  created_date DATE,
  timestamp TIMESTAMP,
  global_entity_id STRING,
  vendor_id STRING,
  orders_queued INT64
)
PARTITION BY created_date
