CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._porygon_drive_time`
PARTITION BY created_date AS
WITH porygon_drive_time_polygons AS (
  SELECT * EXCEPT(_row_number)
    FROM (
      SELECT *
        , ROW_NUMBER() OVER (PARTITION BY country_code, restaurant_id, vehicle_profile, time ORDER BY updated_at DESC) AS _row_number
      FROM `{{ params.project_id }}.dl.porygon_drive_time_polygons`
    )
  WHERE _row_number = 1
), final_dataset AS (
  SELECT o.country_code
    , o.created_date
    , o.created_at
    , de.city_id
    , de.id AS delivery_id
    , p.vehicle_profile AS vehicle_profile
    , p.id AS drive_time_polygon_id
    , o.order_id
    , o.timezone
    , p.time AS drive_time_value
    , b.vendor_code
    , b.porygon_platform
    , ST_CONTAINS(SAFE.ST_GEOGFROMTEXT(p.shape_wkt), o.customer.location) AS is_in_polygon
    , ROW_NUMBER() OVER (PARTITION BY o.country_code, de.id, p.vehicle_profile ORDER BY p.time) AS _row_number
  FROM `{{ params.project_id }}.cl._orders` o
  LEFT JOIN `{{ params.project_id }}.cl._deliveries` d ON o.country_code = d.country_code
    AND o.order_id = d.order_id
  LEFT JOIN UNNEST (d.deliveries) de ON de.is_primary
  LEFT JOIN `{{ params.project_id }}.cl.vendors` b ON o.country_code = b.country_code
    AND o.vendor_id = b.vendor_id
  LEFT JOIN porygon_drive_time_polygons p ON b.vendor_code = p.restaurant_id
    AND o.country_code = p.country_code
    AND ST_CONTAINS(SAFE.ST_GEOGFROMTEXT(p.shape_wkt), o.customer.location)
  WHERE de.delivery_status = 'completed'
), theoretical_porygon_data AS (
  SELECT country_code
    , created_date
    , created_at
    , order_id
    , timezone
    , vendor_code
    , vehicle_profile
    , drive_time_value
    , porygon_platform
  FROM final_dataset
  WHERE _row_number = 1
)
SELECT f.country_code
  , f.created_date
  , f.created_at
  , f.order_id
  , f.timezone
  , ARRAY_AGG(
      STRUCT(f.vehicle_profile
        , f.drive_time_value
        , a.vehicle_profile AS active_vehicle_profile
    )) AS porygon
FROM theoretical_porygon_data f
LEFT JOIN `{{ params.project_id }}.cl._porygon_vehicle_profile_versions` v ON f.country_code = v.country_code
  AND f.vendor_code = v.vendor_code
  AND f.porygon_platform = v.platform
LEFT JOIN UNNEST (vehicle_profile_history) a ON f.created_at BETWEEN a.active_from AND a.active_to
-- NULL values are causing duplicates when aggregated and we do not need to show NULL vehicle profiles if they do not
-- exist at the time the order was created. Ignoring NULLs during array aggregations will also remove NULL values that
-- are present in the vehicle_profile column.
WHERE a.vehicle_profile IS NOT NULL
GROUP BY 1, 2, 3, 4, 5
