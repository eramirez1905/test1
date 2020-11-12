-- stream_table.sql
CREATE OR REPLACE VIEW `{{ params.target_project_id }}.{{ params.view_dataset }}.{{ params.table_name }}` AS
SELECT * EXCEPT(_ingested_at {% if params.hidden_columns| length > 0 -%} , {{ params.hidden_columns| join(', ') }} {%- endif %})
  , created_at AS merge_layer_created_at
  , _ingested_at AS merge_layer_updated_at
FROM `{{ params.project_id }}.{{ params.dataset }}.{{ params.table_name }}`
