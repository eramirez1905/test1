CREATE TEMP FUNCTION degreeOfLonInMeters(lat FLOAT64) AS (
  -- https://gis.stackexchange.com/questions/2951/algorithm-for-offsetting-a-latitude-longitude-by-some-amount-of-meters
  COS(lat)*111111
);

CREATE TEMP FUNCTION get_bbox_lon_lat(linestring STRING)
RETURNS ARRAY<FLOAT64>
LANGUAGE js AS """
var coords = JSON.parse(linestring).coordinates;
var lons = coords.map(coord => coord[0]);
var lats = coords.map(coord => coord[1]);
return [
  Math.min(...lons),
  Math.max(...lons),
  Math.min(...lats),
  Math.max(...lats)
];
""";

CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._zone_optimisation_geohashes`
PARTITION BY created_date
CLUSTER BY country_code, zone_id AS
WITH zones_shape AS (
  SELECT country_code
    , z.id as zone_id
    , z.shape AS shape_geojson
  FROM `{{ params.project_id }}.cl.countries` c
  LEFT JOIN UNNEST (cities) ci
  LEFT JOIN UNNEST (zones) z ON ci.id = z.city_id
  WHERE z.id IS NOT NULL
), lat_long_data AS (
  SELECT country_code
    , zone_id
    , shape_geojson
    , get_bbox_lon_lat(ST_ASGEOJSON(ST_BOUNDARY(shape_geojson))) AS lon_lat
  FROM zones_shape
), lat_long_buffers AS (
  SELECT country_code
    , zone_id
    -- 1000m buffer
    , 1000/degreeOfLonInMeters(lon_lat[OFFSET(2)]) AS buffer_long
    , 1000/111111 AS buffer_lat
    , lon_lat
  FROM lat_long_data
), lat_long_buffer_data AS (
  SELECT country_code
    , zone_id
    , lon_lat[OFFSET(0)] - buffer_long AS min_lon
    , lon_lat[OFFSET(2)] - buffer_lat AS min_lat
    , lon_lat[OFFSET(1)] + buffer_long AS max_lon
    , lon_lat[OFFSET(3)] + buffer_lat AS max_lat
  FROM lat_long_buffers
), geohash_creation AS (
  SELECT country_code
    , zone_id
    -- Geohash Precision of 7 is the smallest useful value even if we increase size of geohashes.
    , ST_GEOHASH(ST_GEOGPOINT(lon, lat), 7) AS geohash
  FROM lat_long_buffer_data
  -- If we change the geohash precision, then 0.000315 will need to be re-evaluated
  LEFT JOIN UNNEST(GENERATE_ARRAY(min_lat, max_lat, 0.000315)) lat
  LEFT JOIN UNNEST(GENERATE_ARRAY(min_lon, max_lon, 0.000315)) lon
  WHERE (NOT IS_NAN(min_lat))
    AND (NOT IS_NAN(max_lat))
    AND (NOT IS_NAN(min_lon))
    AND (NOT IS_NAN(max_lon))
), geohash_data AS (
  SELECT country_code
    , zone_id
    , geohash
  FROM geohash_creation
  GROUP BY 1, 2, 3
), zone_geohash AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY) AS created_date
    , z.country_code
    , z.zone_id
    , g.geohash
    , ST_ASGEOJSON(ST_GEOGPOINTFROMGEOHASH(g.geohash)) AS geo_point
  FROM geohash_data g
  LEFT JOIN zones_shape z ON z.zone_id = g.zone_id
    AND ST_CONTAINS(shape_geojson, ST_GEOGPOINTFROMGEOHASH(geohash))
  WHERE z.zone_id IS NOT NULL
)
SELECT created_date
  , country_code
  , zone_id
  , geohash
  , geo_point
FROM zone_geohash
