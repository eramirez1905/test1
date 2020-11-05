SELECT
  *
{%- if params.extra_columns %}
  , {{ params.extra_columns | join (', ') }}
{%- endif %}
  , TIMESTAMP('{{ macros.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f") }}') AS _ingested_at
  , TIMESTAMP('{{ execution_date }}') AS merge_layer_run_from
FROM `{{ params.project_id }}.{{ params.dataset }}.{{ params.table_name }}_{{ ts_nodash }}`
