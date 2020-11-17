CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._zone_optimisation_geohash_sessions`
PARTITION BY created_date
CLUSTER BY country_code, zone_id AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 31 DAY) AS start_date
    , DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY) AS end_date
), sessions_data AS (
  SELECT date
    , sessionId as session_id
    , SAFE.ST_GEOGPOINT(SAFE_CAST(cdlongitude AS FLOAT64), SAFE_CAST(cdlatitude AS FLOAT64)) AS geo_point
    , totalTransactions AS total_transactions
    , isTransaction AS has_transaction
    , MaxShopsShown AS count_restaurants
  FROM `{{ params.project_id }}.dl.digital_analytics_sessions_location_details`
  WHERE partitiondate >= (SELECT start_date FROM parameters)
    AND partitiondate <= (SELECT end_date FROM parameters)
    AND LOWER(expeditiontype) = 'delivery'
), zones_shape AS (
  SELECT country_code
    , z.id as zone_id
    , z.shape
  FROM `{{ params.project_id }}.cl.countries` c
  LEFT JOIN UNNEST (cities) ci
  LEFT JOIN UNNEST (zones) z ON ci.id = z.city_id
    AND z.id IS NOT NULL
), sessions_in_zones AS (
  SELECT z.country_code
    , z.zone_id
    , s.date
    , s.session_id
    , s.geo_point
    , s.total_transactions
    , s.has_transaction
    , s.count_restaurants
   FROM sessions_data s
   JOIN zones_shape z ON ST_CONTAINS(z.shape, s.geo_point)
), sessions_geohash AS (
  SELECT country_code
    , zone_id
    , IF(has_transaction IS TRUE, 1, 0) AS has_transaction
    , total_transactions
    , count_restaurants
    -- Geohash Precision of 7 is the smallest useful value even if we increase size of geohashes.
    , ST_GEOHASH(geo_point, 7) AS point_gh
  FROM sessions_in_zones
), agg_sessions AS (
  SELECT g.country_code
    , g.zone_id
    , g.geohash
    , AVG(s.has_transaction) AS cvr
    , SUM(s.total_transactions) AS total_transactions
    , COUNT(s.point_gh) AS count_sessions
    , (SELECT end_date FROM parameters) AS created_date
  FROM `{{ params.project_id }}.cl._zone_optimisation_geohashes` g
  LEFT JOIN sessions_geohash s ON g.country_code = s.country_code
    AND g.zone_id = s.zone_id
    -- Spatial Join
    AND g.geohash = s.point_gh
  GROUP BY 1, 2, 3
)
SELECT country_code
  , zone_id
  , geohash
  , ROUND(cvr, 3) AS cvr
  , total_transactions
  , count_sessions
  , created_date
FROM agg_sessions
