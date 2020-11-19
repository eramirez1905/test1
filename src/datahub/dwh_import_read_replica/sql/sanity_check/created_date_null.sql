SELECT COUNTIF({{ params.time_partitioning }} IS NULL) = 0 AS time_partitioning_null_count
FROM `{{ params.project_id }}.{{ params.dataset }}.{{ params.table_name }}`
WHERE {{ params.time_partitioning }} IS NULL
