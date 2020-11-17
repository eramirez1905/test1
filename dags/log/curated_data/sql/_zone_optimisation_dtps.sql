CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._zone_optimisation_dtps`
PARTITION BY created_date
CLUSTER BY country_code, zone_id AS
WITH zone_shape AS (
  -- Taking shape of the zone
  SELECT country_code
    , z.id AS zone_id
    , ST_GEOGFROMGEOJSON(z.zone_shape.geojson) AS shape
  FROM `{{ params.project_id }}.cl.countries` c
  LEFT JOIN UNNEST (cities) ci
  LEFT JOIN UNNEST (zones) z ON z.is_active
    AND z.city_id = ci.id
), restaurants_zone AS(
  -- Considering restaurants that fall inside the zone
  SELECT r.country_code AS country_code
    , r.platform AS platform
    , z.zone_id
    , CAST(r.id AS STRING) AS restaurant_id
  FROM `{{ params.project_id }}.dl.porygon_restaurants` r
  LEFT JOIN zone_shape z ON z.country_code = r.country_code
    AND ST_CONTAINS(z.shape, ST_GEOGFROMGEOJSON(r.location_geojson))
  WHERE z.zone_id IS NOT NULL
    AND r.is_active
), drive_time_polygons AS (
  SELECT created_date
    , country_code
    , platform
    , restaurant_id
    , vehicle_profile
    , time
    , shape_geojson
    , updated_at
    , ROW_NUMBER() OVER (PARTITION BY country_code, platform, restaurant_id, vehicle_profile, time ORDER BY updated_at DESC ) AS rank
  FROM `{{ params.project_id }}.dl.porygon_drive_time_polygons`
), vendor_drive_time_polygons AS (
  SELECT d.created_date
    , r.country_code
    , r.platform
    , r.zone_id
    , r.restaurant_id as vendor_code
    , d.vehicle_profile AS vehicle_profile
    , d.time AS drive_time
    , SAFE.ST_GEOGFROMGEOJSON(shape_geojson) AS shape
  FROM drive_time_polygons d
  LEFT JOIN restaurants_zone r ON r.country_code = d.country_code
    AND r.platform = d.platform
    AND r.restaurant_id = d.restaurant_id
  WHERE r.restaurant_id IS NOT NULL
    AND rank = 1
)
SELECT created_date
  , country_code
  , platform
  , zone_id
  , vendor_code
  , vehicle_profile
  , drive_time
  , shape
FROM vendor_drive_time_polygons
