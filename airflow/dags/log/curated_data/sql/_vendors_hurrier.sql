CREATE TEMP FUNCTION parse(json STRING)
RETURNS ARRAY<STRUCT <latitude NUMERIC, longitude NUMERIC>>
LANGUAGE js AS """
  return JSON.parse(json);
""";

CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._vendors_hurrier` AS
WITH hurrier_platforms AS (
  SELECT country_code
    , entity_id
    , hurrier_platforms
    , country_iso
  FROM `{{ params.project_id }}.cl.countries` c
  LEFT JOIN UNNEST(c.platforms) platform
  LEFT JOIN UNNEST(platform.hurrier_platforms) hurrier_platforms
), addresses AS (
  SELECT a.id
    , a.country_code
    , a.city_id
    , STRUCT(ci.id
        , ci.name
      ) AS city
  FROM `{{ params.project_id }}.cl._addresses` a
  LEFT JOIN `{{ params.project_id }}.cl.countries` c ON a.country_code = c.country_code
  LEFT JOIN UNNEST(c.cities) ci ON ci.id = a.city_id
), hurrier_vendors AS (
  SELECT v.* EXCEPT(last_provided_location, location_history)
    , a.city
    , p.country_iso
    , COALESCE(p.entity_id, f.entity_id, v.global_entity_id) AS entity_id
    , IF(v.unn_code = '0', f.vendor_code, v.unn_code) AS vendor_code
    , SAFE.ST_GEOGPOINT(
        CAST(JSON_EXTRACT_SCALAR(v.last_provided_location, '$.last_longitude') AS NUMERIC),
        CAST(JSON_EXTRACT_SCALAR(v.last_provided_location, '$.last_latitude') AS NUMERIC)
      ) AS last_provided_location
    , ARRAY(SELECT SAFE.ST_GEOGPOINT(longitude, latitude) FROM UNNEST(parse(location_history))) AS location_history
  FROM `{{ params.project_id }}.dl.hurrier_businesses` v
  LEFT JOIN hurrier_platforms p ON v.country_code = p.country_code
    AND COALESCE(v.platform, v.global_entity_id) = p.hurrier_platforms
  LEFT JOIN `{{ params.project_id }}.cl._vendors_hurrier_fix_vendor_codes` f ON f.country_code = v.country_code
    AND f.vendor_id = v.id
  LEFT JOIN addresses a ON a.country_code = v.country_code
    AND a.id = v.address_id
), vendors AS (
  SELECT *
    , ROW_NUMBER() OVER (PARTITION BY entity_id, vendor_code ORDER BY updated_at DESC) AS _row_number
  FROM hurrier_vendors v
  WHERE entity_id IS NOT NULL
)
SELECT v.entity_id
  , v.vendor_code
  , v.name
  , ANY_VALUE(v.last_provided_location) AS last_provided_location
  , ARRAY_AGG(STRUCT(h.country_code
      , h.id
      , h.city
      , h.name
      , h.order_value_limit
      , h.created_at
      , h.updated_at
      , h.last_provided_location
      , h.stacking_mode
      , h.custom_stacking_value
      , h.max_stacked_orders
    ) ORDER BY h.updated_at) hurrier
  , ARRAY_CONCAT_AGG(h.location_history) AS location_history
FROM vendors v
LEFT JOIN hurrier_vendors h USING(entity_id, vendor_code)
WHERE v._row_number = 1
GROUP BY 1,2,3
