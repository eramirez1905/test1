CREATE TEMP FUNCTION convert_legacy_platform_id(legacy_platform_id STRING, legacy_vendor_code  STRING)
RETURNS STRING
AS (
  CASE
    WHEN LOWER(legacy_platform_id) = 'foodora' AND SUBSTR(legacy_vendor_code, 0, 2)
        IN ('AT','CA','FI','NO','SE')
      THEN CONCAT('FO_', SUBSTR(legacy_vendor_code, 0, 2))
    WHEN LOWER(legacy_platform_id) = 'foodora' AND SUBSTR(legacy_vendor_code, 0, 2)
        IN ('MY','PH','PK','HK','RO','SG','TH','TW','BD','BG','LA','MM','KH')
      THEN CONCAT('FP_', SUBSTR(legacy_vendor_code, 0, 2))
    WHEN SUBSTR(legacy_vendor_code, 0, 2) = 'PO'
      THEN 'PO_FI'
    WHEN SUBSTR(legacy_vendor_code, 0, 2) = 'OP'
      THEN 'OP_SE'
    ELSE UPPER(legacy_platform_id)
  END
);

CREATE TEMP FUNCTION convert_legacy_vendor_code(legacy_platform_id STRING, legacy_vendor_code  STRING)
RETURNS STRING
AS (
  IF(LOWER(legacy_platform_id) = 'foodora',
    SPLIT(legacy_vendor_code, '-')[SAFE_OFFSET(1)],
    legacy_vendor_code
  )
);

CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._vendors_rps_is_monitor_enabled` AS
WITH entities AS (
  SELECT * EXCEPT(_entity_id_counter)
    , (MAX(_entity_id_counter) OVER (PARTITION BY rps_platform) <= 1) AS is_1on1_mapping
  FROM (
    SELECT e.region_short_name
      , p.entity_id
      , e.country_iso
      , rps_platform
      , COUNT(1) OVER (PARTITION BY rps_platform) AS _entity_id_counter
    FROM `{{ params.project_id }}.cl.entities` e
    LEFT JOIN UNNEST(platforms) p
    LEFT JOIN UNNEST(p.rps_platforms) rps_platform
  )
-- This is to accommodate joins with monitor settings data
), mapable_entities AS (
  SELECT *
  FROM entities
  WHERE is_1on1_mapping
-- From here onwards is the logic to add is_monitor_enabled logic.
-- reference:
-- https://confluence.deliveryhero.com/pages/viewpage.action?spaceKey=RPSC&title=Configuration+of+Restaurant+Settings
-- iCash vendor monitor settings
), icash_contracts AS (
  SELECT entity_id
    , region
    , vendor_code
    , updated_at
    , rps_global_key
    , is_active
    , is_monitor_enabled AS is_monitor_enabled_from_icash
  FROM `{{ params.project_id }}.cl._vendors_rps_icash` v
-- Vendor Monitor Platform Settings info
), platform_settings AS (
  SELECT UPPER(COALESCE(e.country_iso , pm.countrycode)) AS country_code
    , UPPER(COALESCE(e.entity_id, pm.platformId)) AS entity_id
    , pm.region
    , pm.autoCloseEnabled AS is_autoclose_enabled
    -- Autoclose is the main indicator if a platform activates monitor or not
    , pm.autoCloseEnabled AS is_monitor_enabled_from_platform
    , pm.updated_at AS updated_at
    , ROW_NUMBER() OVER (PARTITION BY pm.countrycode, pm.platformId ORDER BY pm.updated_at DESC) AS _ps_cntr
  FROM `{{ params.project_id }}.hl.restaurant_monitor_platform_settings` pm
  LEFT JOIN entities e ON UPPER(pm.platformId) = e.rps_platform
-- Vendor Monitor Vendor Settings info
), vendor_settings AS (
  SELECT COALESCE(e.entity_id, convert_legacy_platform_id(vm.platform, vm.vendorId)) AS _platform_entity_id
    , convert_legacy_vendor_code(vm.platform, vm.vendorId) AS vendor_code
    , vm.region
    , vm.intendedByPlatform AS is_intended_by_platform
    , vm.hasPosIntegration AS is_pos_integrated
    , vm.autoCloseEnabled AS is_autoclose_enabled
     -- If the flag IntendedbyPlatform true and not an POS integrated vendors,
     -- autoclose will be the identifier, else it is NOT monitor enabled.
    , IF(COALESCE(vm.intendedByPlatform, TRUE) AND NOT COALESCE(vm.hasPosIntegration, FALSE)
        , vm.autoCloseEnabled
        , FALSE
      ) AS is_monitor_enabled_from_vendor
    , vm.updated_at AS updated_at
    , ROW_NUMBER() OVER (PARTITION BY vm.platform, vm.vendorId, vm.region ORDER BY vm.updated_at DESC) AS _vs_cntr
  FROM `{{ params.project_id }}.hl.restaurant_monitor_vendor_settings` vm
  LEFT JOIN mapable_entities e ON convert_legacy_platform_id(vm.platform, vm.vendorId) = rps_platform
  WHERE UPPER(platform) NOT IN ('FOODORA', '9C')
-- Vendor Monitor Activity info
), monitor_activity AS (
  SELECT DISTINCT COALESCE(e.entity_id, convert_legacy_platform_id(m.platformId, m.platformRestaurantId)) AS _platform_entity_id
    , convert_legacy_vendor_code(platformId, platformRestaurantId) AS vendor_code
    , m.region
    , TRUE AS is_monitor_enabled_from_monitor
    -- because the logic behind vendor monitor checks is just to have activities on the last 30 days, 
    -- we find the latest date and always seti it as true
    , MAX(m.eventTime) AS updated_at
    , TRUE AS is_latest
  FROM `{{ params.project_id }}.dl.rps_monitor` m
  LEFT JOIN mapable_entities e ON convert_legacy_platform_id(m.platformId, m.platformRestaurantId) = e.rps_platform
  WHERE UPPER(platformId) NOT IN ('TEST', '9C')
    AND created_date BETWEEN DATE_SUB('{{ next_ds }}', INTERVAL 30 DAY) AND '{{ next_ds }}' 
  GROUP BY 1,2,3,4
), _reference_key_joins AS (
  SELECT *
  FROM (
    SELECT vs.vendor_code
      , vs._platform_entity_id
    FROM vendor_settings vs
    UNION ALL 
    SELECT CAST(NULL AS STRING) AS vendor_code
      , ps.entity_id AS _platform_entity_id
    FROM platform_settings ps
  )
  GROUP BY 1,2
), final_logic AS (
  SELECT DISTINCT *
    , (ROW_NUMBER() OVER (PARTITION BY region, _platform_entity_id, vendor_code ORDER BY updated_at DESC) = 1) AS is_latest
  FROM (
    SELECT COALESCE(ps.region, vs.region) AS region
      , k._platform_entity_id
      , ic.rps_global_key
      , k.vendor_code
      -- The concept of the logic is bottom-up approach.
      -- First we check if there are activity in rps_monitor table,
      -- next we check the vendor settings table. If it is null
      -- then we check Platform settings. If vendor settings is not
      -- null then we use it as is_monitor_enabled flag.
      -- If it everything else is null, then it is not enabled for monitor.
      , CASE
          WHEN COALESCE(ma.is_monitor_enabled_from_monitor, FALSE)
            THEN CASE
              WHEN vs.is_monitor_enabled_from_vendor IS NULL
                THEN COALESCE(ps.is_monitor_enabled_from_platform, FALSE)
              ELSE vs.is_monitor_enabled_from_vendor
              END
            ELSE COALESCE(ic.is_monitor_enabled_from_icash, TRUE) AND COALESCE(ps.is_monitor_enabled_from_platform, FALSE)
          END AS is_monitor_enabled
      , MAX( CASE
            WHEN COALESCE(ma.is_monitor_enabled_from_monitor, FALSE)
              THEN CASE
                WHEN vs.is_monitor_enabled_from_vendor IS NULL
                  THEN COALESCE(ps.updated_at, vs.updated_at, '{{ next_execution_date }}')
                ELSE vs.updated_at
                END
            ELSE COALESCE(ps.updated_at, ic.updated_at)
            END) AS updated_at
    FROM _reference_key_joins k
    LEFT JOIN vendor_settings vs ON k.vendor_code = vs.vendor_code
      AND k._platform_entity_id = vs._platform_entity_id
    LEFT JOIN icash_contracts ic ON k.vendor_code = ic.vendor_code   
      AND IF(k._platform_entity_id <> ic.entity_id, k._platform_entity_id = ic.rps_global_key, vs._platform_entity_id = ic.entity_id)
    LEFT JOIN monitor_activity ma ON ic.vendor_code = ma.vendor_code 
      AND IF(ma._platform_entity_id <> ic.entity_id, ma._platform_entity_id = ic.rps_global_key, ma._platform_entity_id = vs._platform_entity_id)
    LEFT JOIN platform_settings ps ON k._platform_entity_id = ps.entity_id
    GROUP BY 1,2,3,4,5 
   ) 
-- Aggregation scrpt part
), vendor_settings_agg AS (
  SELECT region
  , _platform_entity_id
  , vendor_code
  , ARRAY_AGG(
      STRUCT( STRUCT(is_intended_by_platform, is_pos_integrated, is_autoclose_enabled) AS components
        , is_monitor_enabled_from_vendor
        , updated_at
        , (_vs_cntr = 1) AS is_latest
      ) ORDER BY updated_at DESC
    ) AS vendor_settings
  FROM vendor_settings
  GROUP BY 1,2,3
), monitor_activity_agg AS (
  SELECT region
    , _platform_entity_id
    , vendor_code
    , ARRAY_AGG(
        STRUCT(is_monitor_enabled_from_monitor
          , updated_at AS updated_at
          , is_latest AS is_latest
        ) ORDER BY updated_at DESC
      ) AS monitor_activity
  FROM monitor_activity
  GROUP BY 1,2,3
), platform_settings_agg AS (
  SELECT region
    , entity_id
    , ARRAY_AGG(
        STRUCT( STRUCT(is_autoclose_enabled) AS components
          , is_monitor_enabled_from_platform
          , updated_at
          , (_ps_cntr = 1) AS is_latest
        ) ORDER BY updated_at DESC
      ) AS platform_settings
  FROM platform_settings 
  GROUP BY 1,2
), final_logic_agg AS (
  SELECT region
    , _platform_entity_id
    , vendor_code
    , rps_global_key
    , ARRAY_AGG(
        STRUCT(is_monitor_enabled
          , updated_at
          , is_latest
        ) ORDER BY updated_at DESC
      ) AS final
  FROM final_logic
  GROUP BY 1,2,3,4
)
SELECT * EXCEPT(_row_num)
FROM (
  SELECT fl.region
    , fl._platform_entity_id
    , fl.vendor_code
    , fl.final
    , vs.vendor_settings
    , ma.monitor_activity
    , ps.platform_settings
    , ROW_NUMBER() OVER (PARTITION BY fl.region, fl._platform_entity_id, fl.vendor_code ) AS _row_num
  FROM final_logic_agg fl
  LEFT JOIN vendor_settings_agg vs ON fl.vendor_code = vs.vendor_code 
    AND IF(vs._platform_entity_id <> fl._platform_entity_id, fl.rps_global_key = vs._platform_entity_id, fl._platform_entity_id = vs._platform_entity_id)
  LEFT JOIN monitor_activity_agg ma ON fl.vendor_code = ma.vendor_code 
    AND IF(ma._platform_entity_id <> fl._platform_entity_id, fl.rps_global_key = ma._platform_entity_id, fl._platform_entity_id = ma._platform_entity_id)
  LEFT JOIN platform_settings_agg ps ON ps.entity_id = fl._platform_entity_id
)
WHERE _row_num = 1
