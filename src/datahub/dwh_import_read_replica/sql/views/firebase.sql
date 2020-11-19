-- firebase.sql
CREATE OR REPLACE VIEW `{{ params.target_project_id }}.{{ params.view_dataset }}.{{ params.table_name }}` AS
SELECT
    IF(STARTS_WITH(_TABLE_SUFFIX, 'intraday'),
        PARSE_DATE('%Y%m%d', REPLACE(_TABLE_SUFFIX, 'intraday_', '')),
        PARSE_DATE('%Y%m%d', _TABLE_SUFFIX)
    ) AS created_date
  , TIMESTAMP_MICROS(event_timestamp) AS created_at
  , * {% if params.hidden_columns| length > 0 -%} EXCEPT({{ params.hidden_columns| join(', ') }}) {%- endif %}
FROM `{{ params.source_table }}_*`
