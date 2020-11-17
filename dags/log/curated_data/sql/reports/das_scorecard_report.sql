CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.das_scorecard_report` AS
WITH base AS (
  SELECT v.entity_id
    , v.vendor_code
    , delivery_provider
    , pory.country_code
    , COALESCE(v.address.city_name, hur.city.name) AS city_name
    , hur.city.id AS city_id
    , COALESCE(da.platform, pory.platform) AS platform
    , COALESCE(v.location, hur.last_provided_location, pory.location) AS location
    , COALESCE(v.vehicle_profile, pory.vehicle_profile) AS vehicle_profile
    , da.id AS delivery_area_id
    , COALESCE(his.active_to, '{{ next_ds }}') AS last_updated
    , his.operation_type
    , his.drive_time
    , his.settings.delivery_fee.amount AS delivery_fee
    , his.shape
    , ST_COVERS(his.shape, v.location) AS vendor_in_deliveryarea
    , RANK() OVER (
        PARTITION BY v.entity_id, v.vendor_code, delivery_provider, da.id
        ORDER BY his.active_to DESC, his.operation_type DESC
      ) AS recency_rank
  FROM `{{ params.project_id }}.cl.vendors_v2` v
  LEFT JOIN UNNEST(delivery_provider) delivery_provider
  LEFT JOIN UNNEST(hurrier) hur
  LEFT JOIN UNNEST(delivery_areas) da ON da.is_deleted IS FALSE
  LEFT JOIN UNNEST(porygon) AS pory
  LEFT JOIN UNNEST(da.history) his ON his.operation_type IN ('updated', 'created')
  WHERE v.is_active IS TRUE
    AND pory.country_code IS NOT NULL
    -- removing darkstore pickers to not impact utr as they do not have orders, only working hours
    AND pory.country_code NOT LIKE '%dp%'
), basic_counts AS (
  SELECT base.* EXCEPT(recency_rank)
    , COUNT(delivery_area_id) OVER (PARTITION BY entity_id, vendor_code, delivery_provider) AS count_deliveryareas
    , COUNT(drive_time) OVER (PARTITION BY entity_id, vendor_code, delivery_provider) AS number_drive_time_deliveryareas
    , COUNT(DISTINCT delivery_fee) OVER (PARTITION BY entity_id, vendor_code, delivery_provider) AS count_distinct_delivery_fee
    -- cannot use distinct with location data type, so I am using row_number to only take one row per vendor
    , ROW_NUMBER() OVER (PARTITION BY entity_id, vendor_code, delivery_provider ORDER BY last_updated DESC, vendor_in_deliveryarea DESC) AS ranking
  FROM base
  WHERE recency_rank = 1
-- get distinct info by PK
), core_info AS (
  SELECT entity_id
    , vendor_code
    , delivery_provider
    , country_code
    , city_name
    , city_id
    , platform
    , vehicle_profile
    , LOWER(vehicle_profile) IN ('car', 'bicycle', 'default') AS has_custom_vehicle_profile
    , location
    , vendor_in_deliveryarea
    , count_distinct_delivery_fee
    , number_drive_time_deliveryareas
    , count_deliveryareas
  FROM basic_counts
  WHERE ranking = 1
 -- aggregate shape on PK only
), shape_aggregation AS (
  SELECT entity_id
    , vendor_code
    , delivery_provider
    , ST_UNION_AGG(shape) AS deliveryarea_shape
  FROM base
  GROUP BY 1,2,3
), zone_shape AS (
  SELECT p.entity_id
    , ci.id AS city_id
    , ci.name AS city_name
    , zo.id AS zone_id
    , zo.shape AS zone_shape
  FROM `{{ params.project_id }}.cl.countries`
  LEFT JOIN UNNEST(platforms) p
  LEFT JOIN UNNEST(cities) ci
  LEFT JOIN UNNEST(zones) zo
  WHERE zo.shape IS NOT NULL
    AND zo.is_active IS TRUE
), zone_evaluation AS (
  SELECT ci.entity_id
    , ci.vendor_code
    , ci.delivery_provider
    , ci.city_id
    , ci.city_name
    , COALESCE(zone_city_id.zone_shape, zone_city_name.zone_shape) AS zone_shape
    -- find the zone in which the vendor is located
    , COALESCE(
          -- compare zone shape and location based on city_id join
          ST_COVERS(zone_city_id.zone_shape, ci.location)
          -- compare zone shape and location based on city_name join
          , ST_COVERS(zone_city_name.zone_shape, ci.location)
      ) IS TRUE AS vendor_in_zone
  FROM core_info ci
  LEFT JOIN shape_aggregation shape ON ci.entity_id = shape.entity_id
    AND ci.vendor_code = shape.vendor_code
    AND ci.delivery_provider = shape.delivery_provider
  -- join on city id
  LEFT JOIN zone_shape zone_city_id ON zone_city_id.entity_id = ci.entity_id
    AND zone_city_id.city_id = ci.city_id
    AND ST_COVERS(zone_city_id.zone_shape, ci.location)
  -- join on city name because rps doesn't have city id
  LEFT JOIN zone_shape zone_city_name ON ci.entity_id = zone_city_name.entity_id
    AND LOWER(ci.city_name) = LOWER(zone_city_name.city_name)
    AND ST_COVERS(zone_city_name.zone_shape, ci.location)
), zone_aggregation AS (
  SELECT entity_id
    , vendor_code
    , delivery_provider
    , city_name
    , vendor_in_zone
    , ST_UNION_AGG(zone_shape) AS zone_shape
  FROM zone_evaluation
  WHERE vendor_in_zone IS TRUE
  GROUP BY 1,2,3,4,5

  UNION ALL

  SELECT entity_id
    , vendor_code
    , delivery_provider
    , city_name
    , vendor_in_zone
    , NULL AS zone_shape
   FROM zone_evaluation
   WHERE vendor_in_zone IS FALSE
   GROUP BY 1,2,3,4,5
), clean_final AS (
  SELECT ci.entity_id
    , ci.vendor_code
    , ci.delivery_provider
    , co.region
    , ci.country_code
    , co.country_name
    , ci.city_name
    , ci.platform
    , ci.location
    , ci.vehicle_profile
    , has_custom_vehicle_profile
    , ci.vendor_in_deliveryarea
    , ci.count_deliveryareas
    , ci.count_distinct_delivery_fee > 1 AS has_dynamic_pricing
    , ci.number_drive_time_deliveryareas > 0 AS has_drivetime
    , ci.number_drive_time_deliveryareas
    , za.vendor_in_zone
    -- determine if the delivery area is 100% contained by the zone. **NEED SOLUTION TO THIS**
    , ST_COVEREDBY(shape.deliveryarea_shape, za.zone_shape) AS deliveryarea_in_zone
    -- determine id the delivery area is 100% outside the zone
    , ST_DISJOINT(za.zone_shape, shape.deliveryarea_shape) AS deliveryarea_not_in_zone
  FROM core_info ci
  LEFT JOIN shape_aggregation shape ON ci.entity_id = shape.entity_id
    AND ci.vendor_code = shape.vendor_code
    AND ci.delivery_provider = shape.delivery_provider
  LEFT JOIN zone_aggregation za ON ci.entity_id = za.entity_id
    AND ci.vendor_code = za.vendor_code
    AND ci.delivery_provider = za.delivery_provider
  LEFT JOIN `{{ params.project_id }}.cl.countries` co ON co.country_code = ci.country_code
)
SELECT entity_id
  , vendor_code
  , delivery_provider
  , region
  , country_code
  , country_name
  , city_name
  , platform
  , location
  , vehicle_profile
  , has_custom_vehicle_profile
  , count_deliveryareas
  , vendor_in_deliveryarea IS TRUE AS vendor_in_deliveryarea
  , vendor_in_zone IS TRUE AS vendor_in_zone
  , deliveryarea_in_zone IS TRUE AS deliveryarea_in_zone
  , deliveryarea_not_in_zone IS TRUE AS deliveryarea_not_in_zone
  , number_drive_time_deliveryareas
  , has_drivetime
  , has_dynamic_pricing
FROM clean_final
