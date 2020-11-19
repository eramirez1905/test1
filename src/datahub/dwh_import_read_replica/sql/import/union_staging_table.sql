CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dataset }}.{{ params.table_name }}_{{ execution_date.strftime('%Y%m%d_%H%M%S') }}` AS
SELECT *
FROM `{{ params.project_id }}.{{ params.dataset }}.{{ params.table_name }}_{{ execution_date.strftime('%Y%m%d_%H%M%S') }}_*`
{% if params.filter_by_countries -%}
WHERE country_code IN ({{ params.countries }})
{% endif -%}
