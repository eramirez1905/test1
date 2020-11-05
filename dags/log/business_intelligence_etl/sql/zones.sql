CREATE OR REPLACE table il.zones
PARTITION BY created_date
CLUSTER BY country_code, city_id, zone_id AS
SELECT z.country_code
  , z.city_id
  , z.id AS zone_id
  , z.name AS zone_name
  , z.id AS hurrier_zone_id
  , z.name AS hurrier_zone_name
  , z.starting_geo_id
  , z.geo_id
  , z.geo_json_geojson AS geom
  , CAST(created_at AS DATE) as created_date
FROM `{{ params.project_id }}.ml.hurrier_zones` z
WHERE z.active = true
;
