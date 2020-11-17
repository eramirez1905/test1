-- filter_duplicates.sql
CREATE OR REPLACE VIEW `{{ params.target_project_id }}.{{ params.view_dataset }}.{{ params.table_name }}` AS
SELECT * EXCEPT(_row_number, _ingested_at {% if params.hidden_columns| length > 1 -%} , {{ params.hidden_columns| join(', ') }} {%- endif %})
FROM (
  SELECT *
    , MIN(_ingested_at) OVER row_number_window AS merge_layer_created_at
    , _ingested_at AS merge_layer_updated_at
    , ROW_NUMBER() OVER row_number_window _row_number
  FROM `{{ params.project_id }}.{{ params.dataset }}.{{ params.table_name }}`
  {% if params.partition_time_expiration -%}
  WHERE {{ params.time_partitioning }} >= DATE_SUB('{{ ds }}', INTERVAL {{ params.partition_time_expiration }} DAY)
  {% endif -%}
  WINDOW row_number_window AS (
    PARTITION BY {{ params.partition_by | join(', ') }}
    ORDER BY {{ params.updated_at_column }} DESC, _ingested_at  DESC
  )
)
WHERE _row_number = 1
