SELECT * EXCEPT({{ params.except_columns }})
{% if params.time_columns -%}
    {% for column in params.time_columns -%}
    , CAST(`{{ column }}` AS TIME) AS `{{ column }}`
    {% endfor -%}
{% endif %}
FROM `{{ params.project_id }}.{{ params.dataset }}.{{ params.table_name }}_{{ execution_date.strftime('%Y%m%d_%H%M%S') }}_*`
