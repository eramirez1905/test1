CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_mkt.{{ params.table_name }}`
AS
SELECT region
    , country_iso
    , common_name
    , currency_code
    , ARRAY_AGG(
        STRUCT(
            source_id
            , company_name
            , dwh_source_code AS entity_id
            , dwh_company_id
            , is_active
            , management_entity_group
            , display_name
            , management_entity
        )
    ) AS platforms
FROM `{{ params.project_id }}.dl_mkt.dwh_il_dim_countries`
GROUP BY 1, 2, 3, 4
