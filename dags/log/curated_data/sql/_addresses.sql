CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._addresses`
PARTITION BY created_date
CLUSTER BY country_code, id AS
SELECT da.country_code
  , da.id
  , da.city_id
  , ci.timezone
  , da.created_date
  , SAFE.ST_GEOGPOINT(da.longitude, da.latitude) AS geo_point
FROM `{{ params.project_id }}.dl.hurrier_addresses` da
LEFT JOIN `{{ params.project_id }}.cl.countries` c ON c.country_code = da.country_code
LEFT JOIN UNNEST(c.cities) ci ON ci.id = da.city_id
