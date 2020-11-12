CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_mkt.{{ params.table_name }}` AS
{% for brand_code, _ in params.ncr_brands.items() -%}
SELECT *
FROM `{{ params.project_id }}.dl_mkt.dwh_ncr_braze_campaigns_{{ brand_code }}`
{{ "UNION ALL\n" if not loop.last }}
{%- endfor -%}
