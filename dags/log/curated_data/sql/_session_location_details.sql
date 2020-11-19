CREATE OR REPLACE TABLE cl._session_location_details
PARTITION BY created_date
CLUSTER BY country_code, zone_id AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 31 DAY) AS start_date
), sessions AS (
  SELECT date
    , globalEntityId AS entity_id
    , platform
    , sessionId as session_id
    , SAFE.ST_GEOGPOINT(SAFE_CAST(cdlongitude AS FLOAT64), SAFE_CAST(cdlatitude AS FLOAT64)) AS geo_point
    , isTransaction AS has_transaction
    , totalTransactions
    , MaxShopsShown AS count_restaurants
    , expeditionType AS expedition_type
  FROM `{{ params.project_id }}.dl.digital_analytics_sessions_location_details`
  WHERE partitionDate >= (SELECT start_date FROM parameters)
    AND LOWER(expeditionType) = 'delivery'
), zones AS (
  SELECT country_code
    , z.id AS zone_id
    , z.shape
  FROM `{{ params.project_id }}.cl.countries` c
  LEFT JOIN UNNEST(cities) ci
  LEFT JOIN UNNEST(ci.zones) z ON ci.id = z.city_id
  WHERE z.id IS NOT NULL
)
SELECT z.country_code
  , z.zone_id
  , s.date AS created_date
  , s.entity_id
  , s.platform
  , s.session_id
  , s.geo_point
  , s.has_transaction
  , s.totalTransactions AS total_transactions
  , s.count_restaurants
  , s.expedition_type
FROM sessions s
JOIN zones z ON ST_CONTAINS(z.shape, s.geo_point)
