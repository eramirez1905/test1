CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.dynamic_pricing_config`
CLUSTER BY entity_id AS
WITH vendor_data AS (
  -- Retrieve all available vendors
  SELECT entity_id
    , vendor_code
    , name
    , delivery_provider
    , vertical_type
    , customer_types
    , chain.id AS chain_id
    , chain.name AS chain_name
    , is_active
    , ST_ASBINARY(address.location) AS location_wkb
  FROM `{{ params.project_id }}.cl.vendors_v2`
), vendor_country_config_data AS (
  -- Primary table with the assigned pricing configs and fallback in the country table.
  SELECT vpc.region
    , LOWER(vp.country_code) AS country_code
    , vp.global_entity_id AS entity_id
    , vp.vendor_id AS vendor_code
    , vp.name AS vendor_name
    , vp.chain_name
    , vp.currency
    , vp.active AS is_active
    , vp.key_account AS is_key_account
    , vp.delivery_types
    , vp.vertical_type
    , vp.customer_types
    , vpc.variant
    , vpc.created_date
    , vpc.created_at
    , vpc.updated_at
    , vpc.scheme_id AS vendor_scheme_id
    , cf.scheme_id AS country_scheme_id
    , vp.location_geojson
    , vp.location_wkt
  FROM `{{ params.project_id }}.dl.dynamic_pricing_vendor` vp
  LEFT JOIN `{{ params.project_id }}.dl.dynamic_pricing_vendor_price_config` vpc USING (global_entity_id, vendor_id)
  LEFT JOIN `{{ params.project_id }}.dl.dynamic_pricing_country_fee_configuration` cf USING (country_code)
), pricing_scheme_data AS (
  -- Table with the pricing schemes
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
  SELECT vcc.region
    , vcc.country_code
    , vcc.entity_id
    , vcc.vendor_code
    , vcc.vendor_name
    , vcc.chain_name
    , vcc.currency
    , vcc.is_active
    , vcc.is_key_account
    , vcc.delivery_types
    , vcc.vertical_type
    , vcc.customer_types
    , vcc.variant
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
    , vcc.location_geojson
    , vcc.location_wkt
  FROM vendor_country_config_data vcc
  LEFT JOIN pricing_scheme_data ps ON ps.global_entity_id = vcc.entity_id
    AND ps.scheme_id = vcc.vendor_scheme_id
  LEFT JOIN pricing_scheme_data ps1 ON ps1.global_entity_id = vcc.entity_id
    AND ps1.scheme_id = vcc.country_scheme_id
), pricing_scheme_fallback AS (
  SELECT md.region
    , md.country_code
    , vd.entity_id
    , vd.vendor_code
    , COALESCE(md.vendor_name, vd.name) AS vendor_name
    , vd.chain_id
    , COALESCE(md.chain_name, vd.chain_name) AS chain_name
    , md.currency
    , COALESCE(md.is_active, vd.is_active) AS is_active
    , is_key_account
    , delivery_types
    , COALESCE(md.vertical_type, vd.vertical_type) AS vertical_type
    , COALESCE(md.customer_types, vd.customer_types) AS customer_types
    , variant
    , md.created_date
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
    , md.location_geojson
    , md.location_wkt
    , vd.location_wkb
  FROM vendor_data vd
  LEFT JOIN merge_scheme_data md USING (entity_id, vendor_code)
), delay_fee_threshold_data AS (
  -- Config data for delay fee based on global_entity_id/thresholds
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
  -- Config data for mov based on global_entity_id/thresholds
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
  -- Config data for travel time fee based on global_entity_id/thresholds
  SELECT tc.global_entity_id
    , tcr.configuration_id
    , tcr.travel_time_threshold
    , tcr.travel_time_fee
    , tc.name
    , tcr.created_at
    , tcr.updated_at
  FROM `{{ params.project_id }}.dl.dynamic_pricing_travel_time_fee_configuration_row` tcr
  LEFT JOIN `{{ params.project_id }}.dl.dynamic_pricing_travel_time_fee_configuration` tc USING (region, configuration_id)
), merge_pricing_schemes AS (
  SELECT psf.entity_id
    , psf.vendor_code
    , psf.vendor_name
    , psf.country_code
    , psf.is_active
    , psf.is_key_account
    , psf.chain_id
    , psf.chain_name
    , psf.delivery_types
    , psf.vertical_type
    , psf.customer_types
    , psf.currency
    , psf.variant
    , psf.scheme_id
    , psf.scheme_name
    , psf.is_scheme_fallback
    , psf.created_at
    , psf.updated_at
    , STRUCT(psf.travel_time_fee_configuration_id AS config_id
        , psf.is_travel_time_configuration_fallback AS is_fallback
        , tt.travel_time_threshold
        , tt.travel_time_fee AS fee
        , tt.name
        , tt.created_at
        , tt.updated_at
      ) AS travel_time_fee_config
    , STRUCT(psf.mov_configuration_id AS config_id
        , psf.is_mov_configuration_fallback AS is_fallback
        , mt.travel_time_threshold
        , mt.minimum_order_value
        , mt.name
        , mt.created_at
        , mt.updated_at
      ) AS mov_fee_config
    , STRUCT(psf.delay_fee_configuration_id AS config_id
        , psf.is_delay_fee_configuration_fallback AS is_fallback
        , dt.delay_threshold
        , dt.travel_time_threshold
        , dt.delay_fee AS fee
        , dt.name
        , dt.created_at
        , dt.updated_at
      ) AS delay_fee_config
    , psf.location_geojson
    , psf.location_wkt
    , psf.location_wkb
  FROM pricing_scheme_fallback psf
  LEFT JOIN travel_time_threshold_data tt ON psf.entity_id = tt.global_entity_id
    AND psf.travel_time_fee_configuration_id = tt.configuration_id
  LEFT JOIN mov_threshold_data mt ON psf.entity_id = mt.global_entity_id
    AND psf.mov_configuration_id = mt.configuration_id
  LEFT JOIN delay_fee_threshold_data dt ON psf.entity_id = dt.global_entity_id
    AND psf.delay_fee_configuration_id = dt.configuration_id
)
SELECT entity_id
  , vendor_code
  , vendor_name
  , country_code
  , is_active
  , is_key_account
  , delivery_types
  , vertical_type
  , customer_types
  , currency
  , chain_id
  , chain_name
  , location_geojson
  , location_wkt
  , location_wkb
  , created_at
  , updated_at
  , variant AS price_config_variant
  , scheme_id
  , scheme_name
  , is_scheme_fallback
  , travel_time_fee_config
  , mov_fee_config
  , delay_fee_config
FROM merge_pricing_schemes
