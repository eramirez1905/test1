CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_mkt.{{ params.table_name }}`
AS
SELECT region
    , country_iso
    , LOWER(country_iso) AS country_code
    , common_name
    , currency_code
FROM `{{ params.project_id }}.dl_mkt.dwh_il_dim_countries`
GROUP BY 1, 2, 3, 4, 5
