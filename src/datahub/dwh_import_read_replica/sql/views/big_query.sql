-- big_query.sql
CREATE OR REPLACE VIEW `{{ params.target_project_id }}.{{ params.view_dataset }}.{{ params.table_name }}` AS
SELECT * {% if params.hidden_columns| length > 0 -%} EXCEPT({{ params.hidden_columns| join(', ') }}) {%- endif %}
  {% if params.time_partitioning -%}
    , CAST({{ params.created_at_column }} AS DATE) AS {{ params.time_partitioning }}
  {% endif -%}
FROM `{{ params.source_table }}`
