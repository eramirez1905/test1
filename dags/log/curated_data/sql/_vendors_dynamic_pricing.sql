CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._vendors_dynamic_pricing`
CLUSTER BY entity_id, vendor_code AS
WITH vendor_country_config_data AS (
  -- Retrieve schemes from vendor_config and country_config
  SELECT vpc.region
    , LOWER(vp.country_code) AS country_code
    , vp.global_entity_id AS entity_id
    , vp.vendor_id AS vendor_code
    , vp.name AS vendor_name
    , vp.chain_name
    , vp.currency
    , vp.active AS is_active
    , vp.key_account AS is_key_account
    , vpc.variant
    , vpc.vendor_price_config_id
    , vpc.created_date
    , vpc.created_at
    , vpc.updated_at
    , vpc.scheme_id AS vendor_scheme_id
    , cf.scheme_id AS country_scheme_id
  FROM `{{ params.project_id }}.dl.dynamic_pricing_vendor` vp
  LEFT JOIN `{{ params.project_id }}.dl.dynamic_pricing_vendor_price_config` vpc USING (global_entity_id, vendor_id)
  LEFT JOIN `{{ params.project_id }}.dl.dynamic_pricing_country_fee_configuration` cf USING (country_code)
), pricing_scheme_data AS (
  -- Details of the pricing scheme : travel time, mov and delay config ids
  SELECT ps.global_entity_id
    , ps.scheme_id
    , ps.name
    , ps.travel_time_fee_configuration_id
    , ps.mov_configuration_id
    , ps.delay_fee_configuration_id
    , ps.created_at
    , ps.updated_at
  FROM `{{ params.project_id }}.dl.dynamic_pricing_price_scheme` ps
), merge_scheme_data AS (
  -- Retrieve vendor and country scheme details
  SELECT vcc.region
    , vcc.country_code
    , vcc.entity_id
    , vcc.vendor_code
    , vcc.vendor_name
    , vcc.chain_name
    , vcc.currency
    , vcc.is_active
    , vcc.is_key_account
    , vcc.variant
    , vcc.vendor_price_config_id
    , vcc.created_date
    , vcc.created_at
    , vcc.updated_at
    , vcc.vendor_scheme_id
    , ps.name AS vendor_scheme_name
    , ps.travel_time_fee_configuration_id AS vendor_travel_time_fee_configuration_id
    , ps.mov_configuration_id AS vendor_mov_configuration_id
    , ps.delay_fee_configuration_id AS vendor_delay_fee_configuration_id
    , vcc.country_scheme_id
    , ps1.name AS country_scheme_name
    , ps1.travel_time_fee_configuration_id AS country_travel_time_fee_configuration_id
    , ps1.mov_configuration_id AS country_mov_configuration_id
    , ps1.delay_fee_configuration_id AS country_delay_fee_configuration_id
  FROM vendor_country_config_data vcc
  LEFT JOIN pricing_scheme_data ps ON ps.global_entity_id = vcc.entity_id
    AND ps.scheme_id = vcc.vendor_scheme_id
  LEFT JOIN pricing_scheme_data ps1 ON ps1.global_entity_id = vcc.entity_id
    AND ps1.scheme_id = vcc.country_scheme_id
), pricing_scheme_fallback AS (
  -- Determine fallback if scheme and configs do not exist. Fallback is country scheme details
  SELECT region
    , country_code
    , entity_id
    , vendor_code
    , vendor_name
    , chain_name
    , currency
    , is_active
    , is_key_account
    , variant
    , vendor_price_config_id
    , created_date
    , created_at
    , updated_at
    , COALESCE(vendor_scheme_id, country_scheme_id) AS scheme_id
    , IF(vendor_scheme_id IS NULL, TRUE, FALSE) AS is_scheme_fallback
    , COALESCE(vendor_scheme_name, country_scheme_name) AS scheme_name
    , COALESCE(vendor_travel_time_fee_configuration_id, country_travel_time_fee_configuration_id) AS travel_time_fee_configuration_id
    , IF(vendor_travel_time_fee_configuration_id IS NULL, TRUE, FALSE) AS is_travel_time_configuration_fallback
    , COALESCE(vendor_mov_configuration_id, country_mov_configuration_id) AS mov_configuration_id
    , IF(vendor_mov_configuration_id IS NULL, TRUE, FALSE) AS is_mov_configuration_fallback
    , COALESCE(vendor_delay_fee_configuration_id, country_delay_fee_configuration_id) AS delay_fee_configuration_id
    , IF(vendor_delay_fee_configuration_id IS NULL, TRUE, FALSE) AS is_delay_fee_configuration_fallback
  FROM merge_scheme_data
), delay_fee_threshold_data AS (
  -- Config data for delay fee based on global_entity_id/configuration_id
  SELECT dc.global_entity_id
    , dcr.configuration_id
    , dcr.delay_threshold
    , dcr.travel_time_threshold
    , dcr.delay_fee
    , dc.name
    , dcr.created_at
    , dcr.updated_at
  FROM `{{ params.project_id }}.dl.dynamic_pricing_delay_fee_configuration_row` dcr
  LEFT JOIN `{{ params.project_id }}.dl.dynamic_pricing_delay_fee_configuration` dc USING (region, configuration_id)
), mov_threshold_data AS (
  -- Config data for mov based on global_entity_id/configuration_id
  SELECT mc.global_entity_id
    , mcr.configuration_id
    , mcr.travel_time_threshold
    , mcr.minimum_order_value
    , mc.name
    , mcr.created_at
    , mcr.updated_at
  FROM `{{ params.project_id }}.dl.dynamic_pricing_mov_configuration_row` mcr
  LEFT JOIN `{{ params.project_id }}.dl.dynamic_pricing_mov_configuration` mc USING (region, configuration_id)
), travel_time_threshold_data AS (
  -- Config data for travel time fee based on global_entity_id/configuration_id
  SELECT tc.global_entity_id
    , tcr.configuration_id
    , tcr.travel_time_threshold
    , tcr.travel_time_fee
    , tc.name
    , tcr.created_at
    , tcr.updated_at
  FROM `{{ params.project_id }}.dl.dynamic_pricing_travel_time_fee_configuration_row` tcr
  LEFT JOIN `{{ params.project_id }}.dl.dynamic_pricing_travel_time_fee_configuration` tc USING (region, configuration_id)
), merge_pricing_configs AS (
  SELECT psf.entity_id
    , psf.vendor_code
    , psf.is_active
    , psf.is_key_account
    , psf.currency
    , psf.variant
    , psf.vendor_price_config_id
    , psf.scheme_id
    , psf.scheme_name
    , psf.is_scheme_fallback
    , psf.created_at
    , psf.updated_at
    , STRUCT(psf.travel_time_fee_configuration_id AS id
        , tt.name
        , psf.is_travel_time_configuration_fallback AS is_fallback
        , tt.travel_time_threshold AS threshold
        , tt.travel_time_fee AS fee
        , tt.created_at
        , tt.updated_at
      ) AS travel_time_config
    , STRUCT(psf.mov_configuration_id AS id
        , mt.name
        , psf.is_mov_configuration_fallback AS is_fallback
        , mt.travel_time_threshold
        , mt.minimum_order_value
        , mt.created_at
        , mt.updated_at
      ) AS mov_config
    , STRUCT(psf.delay_fee_configuration_id AS id
        , dt.name
        , psf.is_delay_fee_configuration_fallback AS is_fallback
        , dt.delay_threshold
        , dt.travel_time_threshold
        , dt.delay_fee AS fee
        , dt.created_at
        , dt.updated_at
      ) AS delay_config
  FROM pricing_scheme_fallback psf
  LEFT JOIN travel_time_threshold_data tt ON psf.entity_id = tt.global_entity_id
    AND psf.travel_time_fee_configuration_id = tt.configuration_id
  LEFT JOIN mov_threshold_data mt ON psf.entity_id = mt.global_entity_id
    AND psf.mov_configuration_id = mt.configuration_id
  LEFT JOIN delay_fee_threshold_data dt ON psf.entity_id = dt.global_entity_id
    AND psf.delay_fee_configuration_id = dt.configuration_id
), aggregate_pricing_configs AS (
  -- Create aggregates for the configs
  SELECT entity_id
     , vendor_code
     , scheme_id
     , vendor_price_config_id
     , created_at
     , updated_at
     , ARRAY_AGG(
        STRUCT(travel_time_config
          , mov_config
          , delay_config
       )) AS pricing_config
   FROM merge_pricing_configs
   GROUP BY 1, 2, 3, 4, 5, 6
), aggregate_pricing_schemes AS (
  -- Each scheme can have multiple created/updated timestamps.
  SELECT psf.entity_id
     , psf.vendor_code
     , psf.scheme_id
     , ARRAY_AGG(
        STRUCT(psf.vendor_price_config_id AS id
          , psf.created_at AS vendor_config_created_at
          , psf.updated_at AS vendor_config_updated_at
          , psf.pricing_config
       )) AS vendor_config
   FROM aggregate_pricing_configs psf
   GROUP BY 1, 2, 3
), pricing_data AS (
  -- If there are vendors with more than one created_at/updated_at in the CTE pricing_scheme_fallback,
  -- the data is duplicated while creating the `dps` record in the next CTE create_dps_aggregate. In order
  -- to avoid the duplicates, a DISTINCT of the records is taken
  SELECT DISTINCT entity_id
     , vendor_code
     , is_active
     , is_key_account
     , currency
     , variant
     , scheme_id
     , scheme_name
     , is_scheme_fallback
  FROM pricing_scheme_fallback
), create_dps_aggregate AS (
  SELECT psf.entity_id
     , psf.vendor_code
     , ARRAY_AGG(
       STRUCT(psf.is_active
         , psf.is_key_account
         , psf.currency
         , psf.variant
         , psf.scheme_id
         , psf.scheme_name
         , psf.is_scheme_fallback
         , aps.vendor_config
     )) AS dps
   FROM pricing_data psf
   LEFT JOIN aggregate_pricing_schemes aps USING (entity_id, vendor_code, scheme_id)
   GROUP BY 1, 2
)
SELECT entity_id
  , vendor_code
  , dps
FROM create_dps_aggregate
