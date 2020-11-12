CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._porygon_drive_time_polygons`
CLUSTER BY country_code, platform, vendor_code AS
WITH polygon_drive_time_data AS (
  SELECT * EXCEPT (_row_number)
  FROM (
    SELECT country_code
      , platform
      , created_at
      , updated_at
      , restaurant_id AS vendor_code
      , vehicle_profile
      , time AS drive_time
      , SAFE.ST_GEOGFROMTEXT(shape_wkt) AS shape
      , ROW_NUMBER() OVER (PARTITION BY country_code, restaurant_id, vehicle_profile, time ORDER BY updated_at DESC) AS _row_number
    FROM `{{ params.project_id }}.dl.porygon_drive_time_polygons`
  )
  WHERE _row_number = 1
)
SELECT country_code
  , platform
  , vendor_code
  , ARRAY_AGG(STRUCT(
      vehicle_profile
    , drive_time
    , shape
    , created_at
    , updated_at
  )) AS polygon_drive_times
FROM polygon_drive_time_data
GROUP BY 1, 2, 3
