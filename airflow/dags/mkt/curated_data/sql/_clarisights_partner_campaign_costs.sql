CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_mkt.{{ params.table_name }}`
PARTITION BY date
CLUSTER BY entity_code AS
WITH source_id_to_entity_code AS (
    SELECT source_id
    , entity_id AS entity_code
    FROM `{{ params.project_id }}.cl_mkt.entities`
    LEFT JOIN UNNEST(platforms)
),
dim AS (
    SELECT DISTINCT
        campaign_object_id
        , campaign
        , {{ params.platform_column }} AS platform
        , {{ params.platform_type_column }} AS platform_type
        , source_id
        , entity_code
    FROM `{{ params.project_id }}.dl_mkt.{{ params.partner }}_core_campaign`
    {%- if params.get("join_with_core_account_table") %}
    JOIN `{{ params.project_id }}.dl_mkt.{{ params.partner }}_core_account` USING (account_object_id)
    {%- endif %}
    LEFT JOIN source_id_to_entity_code  USING (source_id)
)
SELECT
    dim.source_id
    , dim.entity_code
    , dim.campaign
    , IF(dim.platform_type IS NULL OR dim.platform_type = '', 'UNSPECIFIED', dim.platform_type) AS platform_type
    , IF(dim.platform IS NULL OR dim.platform = '', 'UNSPECIFIED', dim.platform) AS platform
    , stats.date
    , stats.{{ params.cost_eur_column }} AS cost_eur
FROM dim
RIGHT JOIN `{{ params.project_id }}.dl_mkt.{{ params.partner }}_stats_campaign` AS stats USING(campaign_object_id)
