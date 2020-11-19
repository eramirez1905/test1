CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._vendors_rps_icash` AS
WITH entities AS (
  SELECT region_short_name
    , p.entity_id
    , e.country_iso
    , rps_platform
  FROM `{{ params.project_id }}.cl.entities` e
  LEFT JOIN UNNEST(platforms) p
  LEFT JOIN UNNEST(p.rps_platforms) rps_platform
), countries AS (
  SELECT DISTINCT country_code
    , FIRST_VALUE(c.timezone) OVER (PARTITION BY country_code ORDER BY c.timezone) AS timezone
  FROM `{{ params.project_id }}.cl.countries` co
  CROSS JOIN UNNEST(cities) c
  WHERE country_code IS NOT NULL
), icash_restaurant_2_delivery_platform_dataset AS (
  -- Turkey data from RPS still sits in EU instance, hence a mapping is needed
  SELECT * EXCEPT (region)
    , IF(country_code = 'tr', 'mena', region) AS region
  FROM `{{ params.project_id }}.dl.icash_restaurant_2_delivery_platform` r
), icash_restaurant_2_delivery_platform AS (
  SELECT * EXCEPT(_row_number)
  FROM (
    SELECT r.*
      , ROW_NUMBER() OVER (
          PARTITION BY r.region, r.belongs_to, r.external_id, r.delivery_platform ORDER BY r.dwh_updated_at DESC
        ) AS _row_number
    FROM icash_restaurant_2_delivery_platform_dataset r
    INNER JOIN `{{ params.project_id }}.cl.entities` e ON e.region_short_name = r.region
      AND LOWER(e.country_iso) = r.country_code
  )
  WHERE _row_number = 1
), icash_operator_contract_plan_options_dataset AS (
  SELECT * EXCEPT(region)
    , IF(country_code = 'tr', 'mena', region) AS region
  FROM `{{ params.project_id }}.dl.icash_operator_contract_plan_options`
), icash_operator_contract_plan_options AS (
  SELECT o.region
    , o.country_code
    , o.operator
    , ARRAY_AGG(IF(o.option = 'FORWARD_ORDER_TO_POS_MW', o.value, NULL) IGNORE NULLS)[OFFSET(0)] AS value
    , ARRAY_AGG(CAST(IF(o.option = 'RESTAURANT_MONITOR_ENABLED', o.value, NULL) AS BOOL) IGNORE NULLS)[OFFSET(0)] AS is_monitor_enabled
  FROM icash_operator_contract_plan_options_dataset o
  INNER JOIN `{{ params.project_id }}.cl.entities` e ON e.region_short_name = o.region
    AND LOWER(e.country_iso) = o.country_code
  WHERE o.option IN ('RESTAURANT_MONITOR_ENABLED', 'FORWARD_ORDER_TO_POS_MW')
  GROUP BY 1,2,3
), icash_restaurant AS (
  SELECT r.*
  FROM (
    SELECT * EXCEPT (region)
      , IF(country_code = 'tr', 'mena', region) AS region
    FROM `{{ params.project_id }}.dl.icash_restaurant`
  ) r
  INNER JOIN `{{ params.project_id }}.cl.entities` e ON e.region_short_name = r.region
    AND LOWER(e.country_iso) = r.country_code
), icash_address_restaurant AS (
  SELECT a.*
  FROM (
    SELECT * EXCEPT (region)
      , IF(country_code = 'tr', 'mena', region) AS region
    FROM `{{ params.project_id }}.dl.icash_address_restaurant`
  ) a
  INNER JOIN `{{ params.project_id }}.cl.entities` e ON e.region_short_name = a.region
    AND LOWER(e.country_iso) = a.country_code
), icash_country AS (
  SELECT IF(country_code = 'tr', 'mena', region) AS region
    , * EXCEPT(region)
  FROM `{{ params.project_id }}.dl.icash_country`
), icash_operator AS (
  SELECT * EXCEPT(region)
    , IF(country_code = 'tr', 'mena', region) AS region
  FROM `{{ params.project_id }}.dl.icash_operator`
), icash_contract_plan_options AS (
  SELECT *
  FROM `{{ params.project_id }}.dl.icash_contract_plan_options`
  WHERE option = 'FORWARD_ORDER_TO_POS_MW'
), icash_contracts AS (
  SELECT o.region
    , o.country_code
    , o.id
    , o.code AS operator_code
    , contracts.name
    , CASE
        WHEN contracts.name LIKE '%Hurrier%' OR (contracts.name LIKE '%RAPP%' AND contracts.name NOT LIKE '%RAPP Marketplace%')
          THEN 'OWN_DELIVERY'
        WHEN contracts.name LIKE '%Webkick%' OR contracts.name LIKE '%RAPP%'
          THEN 'VENDOR_DELIVERY'
        ELSE NULL
      END AS delivery_type
    , COALESCE(operator_contract_options.value, contract_plan_options.value) AS value
    , operator_contract_options.is_monitor_enabled
    , o.deleted
    , o.enabled
  FROM icash_operator o
  LEFT JOIN icash_operator_contract_plan_options operator_contract_options ON o.region = operator_contract_options.region
    AND operator_contract_options.country_code = o.country_code
    AND operator_contract_options.operator = o.id
  LEFT JOIN `{{ params.project_id }}.dl.icash_contract_plan` contracts ON contracts.region = o.region
    AND contracts.id = o.contract_plan
  LEFT JOIN icash_contract_plan_options contract_plan_options ON contract_plan_options.region = o.region
    AND contract_plan_options.contract_plan = o.contract_plan
), icash_vendors AS (
  SELECT COALESCE(LOWER(c.iso_code), LOWER(SUBSTR(contracts.operator_code, 0, 2))) AS country_code
    , rdp.region
    , contracts.operator_code
    , rdp.belongs_to AS vendor_id
    , rdp.external_id AS vendor_code
    , r.real_name AS vendor_name
    , GREATEST(rdp.dwh_updated_at, r.dwh_updated_at, ar.dwh_updated_at) AS updated_at
    , contracts.name AS contract_plan
    , contracts.value AS contract_value
    , (rdp.state = 1 AND r.active AND NOT (contracts.deleted OR NOT contracts.enabled)) AS is_active
    , ROW_NUMBER() OVER (PARTITION BY rdp.region, rdp.country_code, rdp.external_id ORDER BY rdp.activated_on DESC) AS _row_number_for_is_active_flag
    , rdp.delivery_platform
    , TRIM(r.timezone) AS timezone
    , contracts.delivery_type
    -- If is_monitor_enabled is not found in contract info from iCash, it will default to TRUE.
    , COALESCE(contracts.is_monitor_enabled, TRUE) AS is_monitor_enabled
    , contracts
    , STRUCT(
          ar.street
        , ar.building
        , ar.zip AS postal_code
        , ar.city AS city_name
        , SAFE.ST_GEOGPOINT(ar.longitude, ar.latitude) AS location
      ) AS address
  FROM icash_restaurant_2_delivery_platform rdp
  LEFT JOIN icash_restaurant r ON rdp.belongs_to = r.id
    AND rdp.country_code = r.country_code
    AND rdp.region = r.region
  LEFT JOIN icash_address_restaurant ar ON rdp.belongs_to = ar.restaurant
    AND rdp.country_code = ar.country_code
    AND rdp.region = ar.region
  LEFT JOIN icash_country c ON ar.country = c.id
    AND rdp.region = c.region
  LEFT JOIN icash_contracts contracts ON contracts.region = rdp.region
    AND contracts.country_code = rdp.country_code
    AND contracts.id = r.operator
  WHERE rdp.external_id IS NOT NULL
    AND contracts.operator_code IS NOT NULL
)
SELECT e.country_iso
  , v.region
  , e.entity_id
  , v.vendor_code
  , GREATEST(v.updated_at, dp.dwh_updated_at) AS updated_at
  , (v.is_active AND v._row_number_for_is_active_flag = 1) AS is_active
  , v.operator_code
  , v.vendor_id
  , v.vendor_name
  , v.contract_plan
  , v.is_monitor_enabled
  , dp.global_key AS rps_global_key
  , dp.name AS delivery_platform
  , CASE
      WHEN contract_value = 1
        THEN 'INDIRECT'
      WHEN contract_value = 2
        THEN 'DIRECT'
      ELSE NULL
    END AS pos_integration_flow
  , IF(v.country_code = 'kr', 'Asia/Seoul', c.timezone) AS timezone
  , v.delivery_type
  , v.address
  , v.contracts
  , 'iCash' AS service
FROM icash_vendors v
LEFT JOIN `{{ params.project_id }}.dl.icash_delivery_platform` dp ON v.delivery_platform = dp.id
  AND v.region = dp.region
INNER JOIN entities e ON e.region_short_name = v.region
  AND e.rps_platform = dp.global_key
  AND LOWER(e.country_iso) = v.country_code
  -- Exclude dummy vendors
  AND e.entity_id NOT IN ('CD_CL', 'CD_CZ', 'CG_DE', 'PY_BR', 'TB_CO', 'TB_DE', 'TB_LB', 'TB_PL', 'FOC_BH')
LEFT JOIN countries c ON v.country_code = c.country_code
