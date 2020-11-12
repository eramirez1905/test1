CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.vendors_v2` AS
WITH vendors_hurrier AS (
  SELECT *
  FROM `{{ params.project_id }}.cl._vendors_hurrier`
), vendors_porygon AS (
  SELECT *
  FROM `{{ params.project_id }}.cl._vendors_porygon`
), vendors_rps AS (
  SELECT *
  FROM `{{ params.project_id }}.cl._vendors_rps`
), entities AS (
  SELECT * FROM (
    SELECT entity_id
      , vendor_code
    FROM vendors_porygon
    UNION ALL
    SELECT entity_id
      , vendor_code
    FROM vendors_hurrier
    UNION ALL
    SELECT entity_id
      , vendor_code
    FROM vendors_rps
  )
  GROUP BY 1,2
), dedup_tes_resturant AS (
  SELECT * EXCEPT (_row_number)
  FROM (
    SELECT external_id AS vendor_code
      , prep_time_config_reset_date
      , platform_country_id
      , id as restaurant_id
      , region
      , created_at
      , updated_at
      , ROW_NUMBER() OVER (PARTITION BY country_code, external_id ORDER BY updated_at DESC) AS _row_number
    FROM `{{ params.project_id }}.dl.tes_restaurant`
    WHERE platform_country_id IS NOT NULL
      AND external_id IS NOT NULL
  )
  WHERE _row_number = 1
), tes_resturant AS (
  SELECT *
  FROM (
    SELECT res.*
      , global_key AS entity_id
      , ROW_NUMBER() OVER (PARTITION BY country_code, vendor_code ORDER BY updated_at DESC) AS _row_number
    FROM dedup_tes_resturant res
    INNER JOIN `{{ params.project_id }}.dl.tes_platform_country` pc ON pc.id = res.platform_country_id
    WHERE pc.global_key IS NOT NULL
  )
  WHERE _row_number = 1
), prep_time_config_strategy_cleaned AS (
  SELECT pc.global_key AS entity_id
    , external_id AS vendor_code
    , prep_time_config_strategy
    , updated_at
    , prep_time_config_strategy != LEAD(prep_time_config_strategy) OVER (PARTITION BY tes.country_code, external_id ORDER BY updated_at DESC) AS _is_valid
  FROM `{{ params.project_id }}.hl.tes_restaurant` tes
  INNER JOIN `{{ params.project_id }}.dl.tes_platform_country` pc ON pc.id = tes.platform_country_id
), prep_time_config_strategy AS (
  SELECT vendor_code
    , entity_id
    , ARRAY_AGG(STRUCT(prep_time_config_strategy AS strategy
        , updated_at
      ) ORDER BY updated_at DESC) AS prep_time_config
  FROM prep_time_config_strategy_cleaned s
  WHERE _is_valid
    OR  _is_valid IS NULL
  GROUP BY 1, 2
), vendors_dynamic_pricing AS (
  SELECT entity_id
    , vendor_code
    , dps
  FROM `{{ params.project_id }}.cl._vendors_dynamic_pricing`
), tes_time_bucket AS (
  SELECT r.entity_id
    , r.vendor_code
    , ARRAY_AGG(STRUCT(t.created_at AS active_from
        , t.updated_at AS active_to
        , t.id
        , t.day_of_week
        , t.time_of_day
        , t.state
        , t.preparation_time
        , t.preparation_buffer
        , t.created_by
        , t.updated_by
      ) ORDER BY t.created_at, t.updated_at ) AS time_buckets
  FROM tes_resturant r
  INNER JOIN `{{ params.project_id }}.dl.tes_time_bucket` t ON t.restaurant_id  = r.restaurant_id
    AND t.region = r.region
  GROUP BY 1,2
), vendors AS (
  SELECT e.entity_id
    , e.vendor_code
    , COALESCE(vendors_porygon.name, vendors_rps.vendor_name, vendors_hurrier.name) AS name
    , vendors_rps.is_monitor_enabled
    , vendors_porygon.cuisines
    , vendors_porygon.tags
    , COALESCE(vendors_porygon.delivery_provider,
       IF(vendors_rps.delivery_type IS NOT NULL, [vendors_rps.delivery_type], NULL)
      ) AS delivery_provider
    , COALESCE(vendors_porygon.vertical_type, vendors_rps.vertical_type) AS vertical_type
    , vendors_porygon.customer_types
    , vendors_porygon.characteristics
    , vendors_porygon.vehicle_profile
    , vendors_porygon.is_active
    , vendors_porygon.is_active_history
    , vendors_porygon.chain
    , vendors_rps.address
    , hurrier
    , rps
    , tes_resturant.prep_time_config_reset_date AS preparation_time_prediction_reset_at
    , vendors_porygon.location
    , vendors_hurrier.last_provided_location
    , vendors_hurrier.location_history
    , vendors_porygon.delivery_areas
    , vendors_porygon.porygon
    , vendors_porygon.polygon_drive_times
    , vendors_dynamic_pricing.dps
    , tes_time_bucket.time_buckets
    , prep_time_config_strategy.prep_time_config
  FROM entities e
  LEFT JOIN vendors_hurrier ON e.entity_id = vendors_hurrier.entity_id
    AND e.vendor_code = vendors_hurrier.vendor_code
  LEFT JOIN vendors_porygon vendors_porygon ON e.entity_id = vendors_porygon.entity_id
    AND e.vendor_code = vendors_porygon.vendor_code
  LEFT JOIN vendors_rps vendors_rps ON e.entity_id = vendors_rps.entity_id
    AND e.vendor_code = vendors_rps.vendor_code
  LEFT JOIN tes_resturant tes_resturant ON e.entity_id = tes_resturant.entity_id
    AND e.vendor_code = tes_resturant.vendor_code
  LEFT JOIN vendors_dynamic_pricing vendors_dynamic_pricing ON e.entity_id = vendors_dynamic_pricing.entity_id
    AND e.vendor_code = vendors_dynamic_pricing.vendor_code
  LEFT JOIN tes_time_bucket tes_time_bucket ON e.entity_id = tes_time_bucket.entity_id
    AND e.vendor_code = tes_time_bucket.vendor_code
  LEFT JOIN prep_time_config_strategy prep_time_config_strategy ON e.entity_id = prep_time_config_strategy.entity_id
    AND e.vendor_code = prep_time_config_strategy.vendor_code
  WHERE e.entity_id IS NOT NULL
    AND e.vendor_code IS NOT NULL
), city_data AS (
  SELECT p.entity_id
    , country_code
    , ci.name AS city_name
    , ci.id AS city_id
    , ST_UNION_AGG(zo.shape) AS city_shape
  FROM `{{ params.project_id }}.cl.countries` co
  LEFT JOIN UNNEST(co.platforms) p
  LEFT JOIN UNNEST(co.cities) ci
  LEFT JOIN UNNEST(ci.zones) zo
  WHERE country_code IS NOT NULL
  GROUP BY 1, 2, 3, 4
), vendor_cities AS (
  SELECT v.entity_id
    , v.vendor_code
    , ARRAY_AGG(STRUCT(
        cd.country_code
      , cd.city_name
      , city_id
    )) AS delivery_areas_location
  FROM vendors v
  LEFT JOIN city_data cd ON v.entity_id = cd.entity_id
    AND ST_CONTAINS(cd.city_shape, v.location) IS TRUE
  GROUP BY 1,2
)
SELECT v.entity_id
    , v.vendor_code
    , v.name
    , v.is_monitor_enabled
    , v.cuisines
    , v.tags
    , v.delivery_provider
    , v.vertical_type
    , v.customer_types
    , v.characteristics
    , v.vehicle_profile
    , v.is_active
    , v.is_active_history
    , v.chain
    , v.address
    , v.hurrier
    , v.rps
    , v.preparation_time_prediction_reset_at
    , v.location
    , v.last_provided_location
    , v.location_history
    , v.delivery_areas
    , vc.delivery_areas_location
    , v.porygon
    , v.polygon_drive_times
    , v.dps
    , v.time_buckets
    , v.prep_time_config            
FROM vendors v
LEFT JOIN vendor_cities vc ON v.entity_id = vc.entity_id
  AND v.vendor_code = vc.vendor_code
