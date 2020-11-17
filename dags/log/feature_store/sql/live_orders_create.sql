CREATE TABLE `{{ params.project_id }}.{{ params.feature_store_dataset }}.{{ params.table_name }}`
(
  country_code STRING,
  zone_id INT64,
  n_orders INT64,
  created_date DATE,
  created_at TIMESTAMP,
)
PARTITION BY created_date
