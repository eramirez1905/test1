-- filter_last_import_only.sql
CREATE OR REPLACE VIEW `{{ params.target_project_id }}.{{ params.view_dataset }}.{{ params.table_name }}` AS
SELECT * EXCEPT(_last_ingested_at, _ingested_at {% if params.hidden_columns| length > 1 -%} , {{ params.hidden_columns| join(', ') }} {%- endif %})
FROM (
  SELECT *
    , MAX(_ingested_at) OVER row_number_window AS _last_ingested_at
    , _ingested_at AS merge_layer_created_at
    , _ingested_at AS merge_layer_updated_at
  FROM `{{ params.project_id }}.{{ params.dataset }}.{{ params.table_name }}`
  WINDOW row_number_window AS (
    PARTITION BY
    {%- if params.keep_last_import_only_partition_by_pk %}
      {{ params.partition_by | join(', ') }}
    {%- else %}
      {{ params.sharding_column }}
    {%- endif %}
  )
)
WHERE _ingested_at = _last_ingested_at
