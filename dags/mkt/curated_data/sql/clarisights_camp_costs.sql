CREATE OR REPLACE VIEW `{{ params.project_id }}.cl_mkt.{{ params.table_name }}`
AS
{% for partner in params.partners -%}
SELECT '{{ partner }}' AS cost_source
    , *
FROM `{{ params.project_id }}.cl_mkt._clarisights_{{ partner }}_campaign_costs`
{%- if not loop.last %}

UNION ALL

{% endif %}
{%- endfor -%}
