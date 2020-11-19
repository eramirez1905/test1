CREATE OR REPLACE TABLE il.cities AS
  SELECT c.country_code AS country_code
    , c.id AS city_id
    , c.name AS city_name
    , c.active AS is_active
    , false AS shyftplan_fetcher_load
    , c.created_at
    , c.updated_at
    , c.timezone
    , CONCAT (LOWER(country_iso), ':', LOWER(REGEXP_REPLACE(c.name, '[^a-zA-Z]', ''))) AS lg_city_id
  FROM `{{ params.project_id }}.ml.hurrier_cities` c
  LEFT JOIN il.countries co USING(country_code)
;
