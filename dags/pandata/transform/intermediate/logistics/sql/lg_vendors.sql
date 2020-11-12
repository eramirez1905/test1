WITH vendors_agg_time_buckets AS (
  SELECT
    vendors.entity_id,
    vendors.vendor_code,
    ARRAY_AGG(
      STRUCT(
        time_buckets.id,
        time_buckets.created_by AS created_by_lg_user_id,
        time_buckets.updated_by AS updated_by_lg_user_id,
        time_buckets.day_of_week,
        time_buckets.time_of_day AS time_of_day_type,
        time_buckets.state,
        time_buckets.preparation_time AS preparation_time_in_seconds,
        time_buckets.preparation_buffer AS preparation_buffer_in_seconds,
        time_buckets.active_from AS active_from_utc,
        time_buckets.active_to AS active_to_utc
      )
    ) AS time_buckets
  FROM `fulfillment-dwh-production.curated_data_shared.vendors_v2` AS vendors
  CROSS JOIN UNNEST (vendors.time_buckets) AS time_buckets
  GROUP BY
    vendors.entity_id,
    vendors.vendor_code
),

vendor_config_agg_pricing_config AS (
 SELECT
    vendors.entity_id,
    vendors.vendor_code,
    dps.scheme_id AS dps_scheme_id,
    dps.variant AS dps_variant,
    vendor_config.id AS vendor_config_id,
    ARRAY_AGG(
      STRUCT(
        STRUCT(
          pricing_config.travel_time_config.id,
          pricing_config.travel_time_config.name,
          pricing_config.travel_time_config.is_fallback,
          pricing_config.travel_time_config.threshold AS threshold_in_minutes,
          pricing_config.travel_time_config.fee AS fee_local,
          pricing_config.travel_time_config.created_at AS created_at_utc,
          pricing_config.travel_time_config.updated_at AS updated_at_utc
        ) AS travel_time_config,
        STRUCT(
          pricing_config.mov_config.id,
          pricing_config.mov_config.name,
          pricing_config.mov_config.is_fallback,
          pricing_config.mov_config.travel_time_threshold AS travel_time_threshold_in_minutes,
          pricing_config.mov_config.minimum_order_value AS minimum_order_value_local,
          pricing_config.mov_config.created_at AS created_at_utc,
          pricing_config.mov_config.updated_at AS updated_at_utc
        ) AS mov_config,
        STRUCT(
          pricing_config.delay_config.id,
          pricing_config.delay_config.name,
          pricing_config.delay_config.is_fallback,
          pricing_config.delay_config.delay_threshold AS delay_limit_threshold_in_minutes,
          pricing_config.delay_config.travel_time_threshold AS travel_time_threshold_in_minutes,
          pricing_config.delay_config.fee AS fee_local,
          pricing_config.delay_config.created_at AS created_at_utc,
          pricing_config.delay_config.updated_at AS updated_at_utc
        ) AS delay_config
      )
    ) AS pricing_config
  FROM `fulfillment-dwh-production.curated_data_shared.vendors_v2` AS vendors
  CROSS JOIN UNNEST (vendors.dps) AS dps
  CROSS JOIN UNNEST (dps.vendor_config) AS vendor_config
  CROSS JOIN UNNEST (vendor_config.pricing_config) AS pricing_config
  GROUP BY
    vendors.entity_id,
    vendors.vendor_code,
    dps.scheme_id,
    dps.variant,
    vendor_config.id
),

dps_agg_vendor_config AS (
 SELECT
    vendors.entity_id,
    vendors.vendor_code,
    dps.scheme_id AS dps_scheme_id,
    dps.variant AS dps_variant,
    ARRAY_AGG(
      STRUCT(
        vendor_config.id,
        vendor_config.vendor_config_created_at AS created_at_utc,
        vendor_config.vendor_config_updated_at AS updated_at_utc,
        vendor_config_agg_pricing_config.pricing_config
      )
    ) AS vendor_config
  FROM `fulfillment-dwh-production.curated_data_shared.vendors_v2` AS vendors
  CROSS JOIN UNNEST (vendors.dps) AS dps
  CROSS JOIN UNNEST (dps.vendor_config) AS vendor_config
  LEFT JOIN vendor_config_agg_pricing_config
         ON vendors.entity_id = vendor_config_agg_pricing_config.entity_id
        AND vendors.vendor_code = vendor_config_agg_pricing_config.vendor_code
        AND dps.scheme_id = vendor_config_agg_pricing_config.dps_scheme_id
        AND dps.variant = vendor_config_agg_pricing_config.dps_variant
        AND vendor_config.id = vendor_config_agg_pricing_config.vendor_config_id
  GROUP BY
    vendors.entity_id,
    vendors.vendor_code,
    dps.scheme_id,
    dps.variant
),

vendors_agg_dps AS (
 SELECT
    vendors.entity_id,
    vendors.vendor_code,
    ARRAY_AGG(
      STRUCT(
        dps.scheme_id,
        dps.variant,
        dps.scheme_name,
        dps.currency,
        dps.is_active,
        dps.is_key_account,
        dps.is_scheme_fallback,
        dps_agg_vendor_config.vendor_config
      )
    ) AS dps
  FROM `fulfillment-dwh-production.curated_data_shared.vendors_v2` AS vendors
  CROSS JOIN UNNEST (vendors.dps) AS dps
  LEFT JOIN dps_agg_vendor_config
         ON vendors.entity_id = dps_agg_vendor_config.entity_id
        AND vendors.vendor_code = dps_agg_vendor_config.vendor_code
        AND dps.scheme_id = dps_agg_vendor_config.dps_scheme_id
        AND dps.variant = dps_agg_vendor_config.dps_variant
  GROUP BY
    vendors.entity_id,
    vendors.vendor_code
),

vendors_agg_porygon AS (
 SELECT
    vendors.entity_id,
    vendors.vendor_code,
    ARRAY_AGG(
      STRUCT(
        porygon.country_code,
        porygon.platform,
        porygon.global_entity_id,
        porygon.name,
        porygon.location,

        porygon.is_active,
        porygon.is_halal,

        porygon.vertical_type,

        porygon.delivery_provider AS delivery_providers,
        porygon.cuisines,
        porygon.tags,
        porygon.customer_types,
        porygon.characteristics,
        porygon.polygon_drive_times, -- no id, so don't unnest to change field name

        porygon.created_at AS created_at_utc,
        porygon.updated_at AS updated_at_utc
      )
    ) AS porygon
  FROM `fulfillment-dwh-production.curated_data_shared.vendors_v2` AS vendors
  CROSS JOIN UNNEST (vendors.porygon) AS porygon
  GROUP BY
    vendors.entity_id,
    vendors.vendor_code

),

vendors_agg_delivery_areas_locations AS (
 SELECT
    vendors.entity_id,
    vendors.vendor_code,
    ARRAY_AGG(
      STRUCT(
        delivery_area_locations.city_id AS lg_city_id,
        delivery_area_locations.country_code,
        delivery_area_locations.city_name
      )
    ) AS delivery_area_locations
  FROM `fulfillment-dwh-production.curated_data_shared.vendors_v2` AS vendors
  CROSS JOIN UNNEST (vendors.delivery_areas_location) AS delivery_area_locations
  GROUP BY
    vendors.entity_id,
    vendors.vendor_code
),

delivery_areas_agg_history AS (
 SELECT
    vendors.entity_id,
    vendors.vendor_code,
    delivery_areas.id AS delivery_area_id,
    ARRAY_AGG(
      STRUCT(
        history.transaction_id,
        history.end_transaction_id,
        history.name,
        history.drive_time AS drive_time_in_minutes,
        history.operation_type,
        history.shape AS shape_geo,
        history.active_from AS active_from_utc,
        history.active_to AS active_to_utc,
        history.edited_by,
        STRUCT(
          STRUCT(
            history.settings.delivery_fee.amount AS amount_local,
            history.settings.delivery_fee.percentage
          ) AS delivery_fee,
          history.settings.delivery_time AS expected_delivery_time_in_minutes,
          history.settings.municipality_tax,
          history.settings.municipality_tax_type,
          history.settings.tourist_tax,
          history.settings.tourist_tax_type,
          history.settings.status,
          history.settings.minimum_value AS minimum_value_local
        ) AS settings
      )
    ) AS history
  FROM `fulfillment-dwh-production.curated_data_shared.vendors_v2` AS vendors
  CROSS JOIN UNNEST (vendors.delivery_areas) AS delivery_areas
  CROSS JOIN UNNEST (delivery_areas.history) AS history
  GROUP BY
    vendors.entity_id,
    vendors.vendor_code,
    delivery_areas.id

),

vendors_agg_delivery_areas AS (
  SELECT
    vendors.entity_id,
    vendors.vendor_code,
    ARRAY_AGG(
      STRUCT(
        delivery_areas.id,
        delivery_areas.country_code,
        delivery_areas.platform,
        delivery_areas.is_deleted,
        delivery_areas_agg_history.history
      )
    ) AS delivery_areas
  FROM `fulfillment-dwh-production.curated_data_shared.vendors_v2` AS vendors
  CROSS JOIN UNNEST (vendors.delivery_areas) AS delivery_areas
  LEFT JOIN delivery_areas_agg_history
         ON vendors.entity_id = delivery_areas_agg_history.entity_id
        AND vendors.vendor_code = delivery_areas_agg_history.vendor_code
        AND delivery_areas.id = delivery_areas_agg_history.delivery_area_id
  GROUP BY
    vendors.entity_id,
    vendors.vendor_code
),

rps_agg_is_monitor_enabled_history AS (
  SELECT
    vendors.entity_id,
    vendors.vendor_code,
    rps.region AS rps_region,
    rps.vendor_id AS rps_vendor_id,
    ARRAY_AGG(
      STRUCT(
        is_monitor_enabled_history.is_monitor_enabled,
        is_monitor_enabled_history.updated_at AS updated_at_utc,
        is_monitor_enabled_history.is_latest
      )
    ) AS is_monitor_enabled_history
  FROM `fulfillment-dwh-production.curated_data_shared.vendors_v2` AS vendors
  CROSS JOIN UNNEST (vendors.rps) AS rps
  CROSS JOIN UNNEST (rps.is_monitor_enabled_history) AS is_monitor_enabled_history
  GROUP BY
    vendors.entity_id,
    vendors.vendor_code,
    rps.region,
    rps.vendor_id
),

vendors_agg_rps AS (
  SELECT
    vendors.entity_id,
    vendors.vendor_code,
    ARRAY_AGG(
      STRUCT(
        rps.region,
        rps.country_iso,
        rps.is_active,
        rps.is_latest,
        rps.operator_code,
        rps.vendor_id AS rps_vendor_id,
        rps.vendor_name,
        rps.contract_plan,
        rps.delivery_type,
        rps.delivery_platform,
        rps.rps_global_key,
        rps.service,
        rps.is_monitor_enabled,
        rps.timezone,
        rps.updated_at AS updated_at_utc,
        rps.client,
        rps.pos,
        rps.address,
        STRUCT(
          rps.contracts.id,
          rps.contracts.country_code,
          rps.contracts.region,
          rps.contracts.operator_code,
          rps.contracts.name,
          rps.contracts.delivery_type,
          rps.contracts.value,
          rps.contracts.deleted AS is_deleted,
          rps.contracts.enabled AS is_enabled
        ) AS contracts,
        rps_agg_is_monitor_enabled_history.is_monitor_enabled_history
      )
    ) AS rps
  FROM `fulfillment-dwh-production.curated_data_shared.vendors_v2` AS vendors
  CROSS JOIN UNNEST (vendors.rps) AS rps
  LEFT JOIN rps_agg_is_monitor_enabled_history
         ON vendors.entity_id = rps_agg_is_monitor_enabled_history.entity_id
        AND vendors.vendor_code = rps_agg_is_monitor_enabled_history.vendor_code
        AND rps.region = rps_agg_is_monitor_enabled_history.rps_region
        AND rps.vendor_id = rps_agg_is_monitor_enabled_history.rps_vendor_id
  GROUP BY
    vendors.entity_id,
    vendors.vendor_code
),

vendors_agg_hurrier AS (
  SELECT
    vendors.entity_id,
    vendors.vendor_code,
    ARRAY_AGG(
      STRUCT(
        hurrier.country_code,
        hurrier.id,
        hurrier.city,
        hurrier.name,
        hurrier.order_value_limit AS order_value_limit_local,
        hurrier.last_provided_location AS last_provided_location_geo,
        hurrier.created_at AS created_at_utc,
        hurrier.updated_at AS updated_at_utc
      )
    ) AS hurrier
  FROM `fulfillment-dwh-production.curated_data_shared.vendors_v2` AS vendors
  CROSS JOIN UNNEST (vendors.hurrier) AS hurrier
  GROUP BY
    vendors.entity_id,
    vendors.vendor_code
)

SELECT
  vendors.vendor_code || '_' || vendors.entity_id AS uuid,
  vendors.entity_id AS lg_entity_id,
  vendors.vendor_code AS code,
  vendors.name,
  vendors.vehicle_profile,
  vendors.location AS location_geo,
  vendors.last_provided_location AS last_provided_location_geo,
  vendors.location_history AS location_history_geos,
  vendors_agg_hurrier.hurrier,
  vendors_agg_rps.rps,
  vendors_agg_delivery_areas.delivery_areas,
  vendors_agg_delivery_areas_locations.delivery_area_locations,
  vendors_agg_porygon.porygon,
  vendors_agg_dps.dps,
  vendors_agg_time_buckets.time_buckets,
FROM `fulfillment-dwh-production.curated_data_shared.vendors_v2` AS vendors
LEFT JOIN vendors_agg_hurrier
       ON vendors.entity_id = vendors_agg_hurrier.entity_id
      AND vendors.vendor_code = vendors_agg_hurrier.vendor_code
LEFT JOIN vendors_agg_rps
       ON vendors.entity_id = vendors_agg_rps.entity_id
      AND vendors.vendor_code = vendors_agg_rps.vendor_code
LEFT JOIN vendors_agg_delivery_areas
       ON vendors.entity_id = vendors_agg_delivery_areas.entity_id
      AND vendors.vendor_code = vendors_agg_delivery_areas.vendor_code
LEFT JOIN vendors_agg_delivery_areas_locations
       ON vendors.entity_id = vendors_agg_delivery_areas_locations.entity_id
      AND vendors.vendor_code = vendors_agg_delivery_areas_locations.vendor_code
LEFT JOIN vendors_agg_porygon
       ON vendors.entity_id = vendors_agg_porygon.entity_id
      AND vendors.vendor_code = vendors_agg_porygon.vendor_code
LEFT JOIN vendors_agg_dps
       ON vendors.entity_id = vendors_agg_dps.entity_id
      AND vendors.vendor_code = vendors_agg_dps.vendor_code
LEFT JOIN vendors_agg_time_buckets
       ON vendors.entity_id = vendors_agg_time_buckets.entity_id
      AND vendors.vendor_code = vendors_agg_time_buckets.vendor_code
