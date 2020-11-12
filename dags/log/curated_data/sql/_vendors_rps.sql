CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._vendors_rps` AS
WITH entities AS (
  SELECT * EXCEPT(_entity_id_counter)
    , (MAX(_entity_id_counter) OVER(PARTITION BY rps_platform) <= 1) AS is_1on1_mapping
  FROM (
    SELECT region_short_name
      , p.entity_id
      , e.country_iso
      , rps_platform
      , COUNT(1) OVER(PARTITION BY rps_platform) AS _entity_id_counter
    FROM `{{ params.project_id }}.cl.entities` e
    LEFT JOIN UNNEST(platforms) p
    LEFT JOIN UNNEST(p.rps_platforms) rps_platform
  )
-- This is to accomodate joins with monitor settings data
), mapable_entities AS (
  SELECT *
  FROM entities
  WHERE is_1on1_mapping
), countries AS (
  SELECT DISTINCT country_code
    , FIRST_VALUE(c.timezone) OVER (PARTITION BY country_code ORDER BY c.timezone) AS timezone
  FROM `{{ params.project_id }}.cl.countries` co
  CROSS JOIN UNNEST(cities) c
  WHERE country_code IS NOT NULL
), dms_vendors AS (
  SELECT * EXCEPT (_row_number)
  FROM (
    SELECT IF(region = 'asia', 'ap', region) AS region
      , SAFE_CAST(restaurantId AS INT64) AS vendor_id
      , IF(metadata.rpsClientAppType = 'WEBKICK', 'GOWIN', metadata.rpsClientAppType) AS client_name
      , created_at
      , ROW_NUMBER() OVER (PARTITION BY region, restaurantId ORDER BY created_at DESC) AS _row_number
    FROM `{{ params.project_id }}.dl.dms_device`
    WHERE SAFE_CAST(restaurantId AS INT64) IS NOT NULL
  )
  WHERE _row_number = 1
), posmw_vendors AS (
  SELECT *
  FROM (
    SELECT posmwv.region
      , SAFE_CAST(posmwv.vendor_code AS INT64) AS vendor_id
      , IF(posmvvit.integration_type <> 'NON_POS', 'POS', NULL) AS client_name
      , posmvc.name AS pos_chain_name
      , posmvc.chain_code AS pos_chain_code
      , CASE
          WHEN posmvvit.integration_type = 'POS_AS_SECONDARY'
            THEN 'INDIRECT'
          WHEN posmvvit.integration_type = 'POS_AS_PRIMARY'
            THEN 'DIRECT'
          WHEN posmvvit.integration_type = 'NON_POS'
            THEN NULL
          ELSE NULL
        END AS pos_integration_flow
      , posmvi.active AS is_pos_integration_active
      , posmvi.code AS pos_integration_code
      , posmvi.name AS pos_integration_name
      , ROW_NUMBER() OVER (PARTITION BY posmwv.region, SAFE_CAST(posmwv.vendor_code AS INT64)) AS _row_number
    FROM `{{ params.project_id }}.dl.posmw_vendor_mapped` posmwv
    LEFT JOIN `{{ params.project_id }}.dl.posmw_chain_mapped` posmvc ON posmwv.chain_mapped_id = posmvc.id
      AND posmwv.region = posmvc.region
    LEFT JOIN `{{ params.project_id }}.dl.posmw_integration` posmvi ON posmvc.integration_id = posmvi.id
      AND posmvc.region = posmvi.region
    LEFT JOIN `{{ params.project_id }}.dl.posmw_vendor_integration_type` posmvvit ON posmwv.vendor_code = posmvvit.vendor_code
      AND posmwv.region = posmvvit.region
  )
  WHERE _row_number = 1
), vendor_verticals AS (
  SELECT DISTINCT entity_id
    , vendor_code
    , vertical_type
  FROM (
    SELECT content.global_entity_id AS entity_id
      , content.vendor_id AS vendor_code
      , content.vertical_type AS vertical_type
      , ROW_NUMBER() OVER (PARTITION BY content.global_entity_id, content.vendor_id ORDER BY timestamp DESC, metadata.ingestion_timestamp DESC) AS _row_number
    FROM `{{ params.project_id }}.dl.data_fridge_vendor_stream`
  )
  WHERE _row_number = 1
), vendors_rps_vendor_service_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl._vendors_rps_vendor_service`
), vendors_rps_icash_dataset AS (
  SELECT * EXCEPT(contracts, service)
    , STRUCT(
        contracts.region
        , contracts.country_code
        , contracts.id
        , contracts.operator_code
        , contracts.name
        , contracts.delivery_type
        , contracts.value
        , contracts.deleted
        , contracts.enabled
      ) AS contracts
    , service
  FROM `{{ params.project_id }}.cl._vendors_rps_icash`
), vendors_rps_is_monitor_enabled_vendor_level AS (
  SELECT a.region
    , a._platform_entity_id
    , a.vendor_code
    , f.is_monitor_enabled
    , a.final AS is_monitor_enabled_history
  FROM `{{ params.project_id }}.cl._vendors_rps_is_monitor_enabled` a
  LEFT JOIN UNNEST(final) f
  WHERE f.is_latest
), vendors_rps_is_monitor_enabled_platform_level AS (
  SELECT a.region
    , a._platform_entity_id
    , f.is_monitor_enabled
    , a.final AS is_monitor_enabled_history
  FROM `{{ params.project_id }}.cl._vendors_rps_is_monitor_enabled` a
  LEFT JOIN UNNEST(final) f
  WHERE f.is_latest
    AND a.vendor_code IS NULL
), vendors AS (
  SELECT *
  FROM (
    SELECT entity_id
      , vendor_code
      , region
      , vendor_id
    FROM vendors_rps_vendor_service_dataset
    UNION ALL
    SELECT entity_id
      , vendor_code
      , region
      , vendor_id
    FROM vendors_rps_icash_dataset
  )
  WHERE entity_id IS NOT NULL
    AND vendor_code IS NOT NULL
    AND region IS NOT NULL
    AND vendor_id IS NOT NULL
  GROUP BY 1,2,3,4
), vendors_latest AS (
  SELECT *
    , ROW_NUMBER() OVER (PARTITION BY entity_id, vendor_code ORDER BY updated_at DESC) AS _row_number
  FROM (
    SELECT COALESCE(vs.country_iso, icv.country_iso) AS country_iso
      , v.region
      , v.entity_id
      , v.vendor_code
      , vv.vertical_type
      , v.vendor_id
      , COALESCE(vs.vendor_name, icv.vendor_name) AS vendor_name
      , COALESCE(vs.is_active, icv.is_active) AS is_active
      , COALESCE(vs.operator_code, icv.operator_code) AS operator_code
      , CASE
          WHEN posmwv.client_name IS NOT NULL AND COALESCE(posmwv.pos_integration_flow, icv.pos_integration_flow) = 'DIRECT'
            THEN posmwv.client_name
          WHEN dms.vendor_id IS NOT NULL
            THEN dms.client_name
          ELSE NULL
        END AS client_name
      , icv.contract_plan
      , IF(mevl.vendor_code IS NULL, mepl.is_monitor_enabled, mevl.is_monitor_enabled) AS is_monitor_enabled
      , IF(mevl.vendor_code IS NULL, mepl.is_monitor_enabled_history, mevl.is_monitor_enabled_history) AS is_monitor_enabled_history
      , COALESCE(vs.rps_global_key, icv.rps_global_key) AS rps_global_key
      , icv.delivery_platform
      , IF(posmwv.client_name IS NOT NULL, COALESCE(posmwv.pos_integration_flow, icv.pos_integration_flow), NULL) AS pos_integration_flow
      , posmwv.pos_integration_name
      , posmwv.pos_integration_code
      , posmwv.is_pos_integration_active
      , posmwv.pos_chain_name
      , posmwv.pos_chain_code
      , COALESCE(vs.timezone, icv.timezone) AS timezone
      , icv.delivery_type
      , COALESCE(vs.address, icv.address) AS address
      , icv.contracts
      , COALESCE(vs.service, icv.service) AS service
      , COALESCE(vs.updated_at, icv.updated_at) AS updated_at
    FROM vendors v
    LEFT JOIN vendors_rps_vendor_service_dataset vs ON v.vendor_code = vs.vendor_code
      AND v.entity_id = vs.entity_id
    LEFT JOIN vendors_rps_icash_dataset icv ON v.vendor_code = icv.vendor_code
      AND v.entity_id = icv.entity_id
    LEFT JOIN posmw_vendors posmwv ON v.vendor_id = posmwv.vendor_id
      AND v.region = posmwv.region
    LEFT JOIN dms_vendors dms ON v.vendor_id = dms.vendor_id
      AND v.region = dms.region
    LEFT JOIN vendor_verticals vv ON v.entity_id = vv.entity_id
      AND v.vendor_code = vv.vendor_code
    LEFT JOIN vendors_rps_is_monitor_enabled_vendor_level mevl ON v.vendor_code = mevl.vendor_code
      AND IF(LENGTH(mevl._platform_entity_id) > 2, v.entity_id = mevl._platform_entity_id, v.region = mevl.region)
    LEFT JOIN vendors_rps_is_monitor_enabled_platform_level mepl ON v.entity_id = mepl._platform_entity_id
  )
), vendors_agg AS (
  SELECT v.entity_id
    , v.vendor_code
    , ARRAY_AGG(
        STRUCT(
          v.region
          , v.country_iso
          , v.is_active
          , (v._row_number = 1) AS is_latest
          , v.operator_code
          , v.vendor_id
          , v.vendor_name
          , v.contract_plan
          , v.timezone
          , v.updated_at
          , STRUCT(
              v.client_name AS name
              , v.pos_integration_flow
            ) AS client
          , STRUCT(
              pos_integration_code AS integration_code
              , pos_integration_name AS integration_name
              , is_pos_integration_active
              , pos_chain_name AS chain_name
              , pos_chain_code AS chain_code
            ) AS pos
          , v.contracts
          , v.delivery_type
          , v.is_monitor_enabled
          , v.is_monitor_enabled_history
          , v.delivery_platform
          , v.rps_global_key
          , v.address
          , v.service
        )
      ) AS rps
  FROM vendors_latest v
  WHERE _row_number = 1
  GROUP BY 1,2
)
SELECT v.entity_id
  , v.vendor_code
  , v.vertical_type
  , v.vendor_name
  , v.client_name
  , v.contract_plan
  , v.delivery_type
  , v.is_monitor_enabled
  , v.address
  , v.updated_at
  , a.rps
FROM vendors_latest v
LEFT JOIN vendors_agg a USING(entity_id, vendor_code)
WHERE _row_number = 1
