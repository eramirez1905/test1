CREATE OR REPLACE TABLE rl.scorecard_infra AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 10 WEEK) AS start_time
    , DATE_SUB('{{ next_ds }}', INTERVAL 4 WEEK) AS start_time_shapes
    , DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY) AS end_time
), deliveries AS (
  SELECT o.country_code
    , d.city_id
    , o.order_id
    , o.is_preorder
    , o.order_status
    , o.timezone
    , o.entity.display_name AS entity_display_name
    , vendor.id AS vendor_id
    , vendor.vendor_code
    , d.id AS delivery_id
    , d.delivery_status
    , d.timings.actual_delivery_time
    , d.rider_dropped_off_at
    , COALESCE(d.pickup_distance_google, d.pickup_distance_manhattan) AS pickup_distance
    , COALESCE(d.dropoff_distance_google, d.dropoff_distance_manhattan) AS dropoff_distance
    , d.created_at
    , t.state
    , t.auto_transition
  FROM cl.orders o
  LEFT JOIN UNNEST(deliveries) d
  LEFT JOIN UNNEST(d.transitions) t
  WHERE o.created_date >= (SELECT start_time FROM parameters)
    AND DATE(d.rider_dropped_off_at, d.timezone) BETWEEN (SELECT start_time FROM parameters) AND (SELECT end_time FROM parameters)
    AND delivery_status = 'completed'
), distances AS (
-------------------- Pickup and dropoff distance --------------------
  SELECT d.country_code
    , d.city_id
    , FORMAT_DATE('%G-%V', DATE(d.rider_dropped_off_at, d.timezone)) AS report_week_local
    , AVG(pickup_distance / 1000) AS pickup_distance
    , AVG(dropoff_distance / 1000) AS dropoff_distance
  FROM deliveries d
  GROUP BY 1, 2, 3
-------------------- vendor density --------------------
), days AS (
   SELECT GENERATE_DATE_ARRAY((SELECT start_time FROM parameters), (SELECT end_time FROM parameters), INTERVAL 1 DAY) AS day
), day_sequence AS (
  SELECT c.country_code
    , c.city_id
    , day
  FROM (
    SELECT pc.country_code
      , ci.id AS city_id
    FROM cl.countries pc
    LEFT JOIN UNNEST(cities) ci
  ) c
  CROSS JOIN days
  LEFT JOIN UNNEST(days.day) AS day
), vendors AS (
  SELECT country_code
    , city_id
    , vendor_id
    , ARRAY_AGG(DISTINCT FORMAT_DATE('%G-%V', DATE(rider_dropped_off_at, timezone))) AS active_weeks
    --- Assign to each vendor an array of weeks where this vendor has delivered at least one order
  FROM deliveries
  GROUP BY 1, 2, 3
), active_vendors AS (
  SELECT FORMAT_DATE('%G-%V', d.day) AS report_week
      , d.country_code
      , d.city_id
      , COUNT(DISTINCT(IF(FORMAT_DATE('%G-%V', d.day) IN UNNEST(v.active_weeks), vendor_id, NULL))) AS vendor_count
      ---- Vendor are considered active if they have completed at least one order on the report week
  FROM day_sequence d
  LEFT JOIN vendors v ON d.country_code = v.country_code
    AND d.city_id = v.city_id
  GROUP BY 1, 2, 3
), active_delivery_areas AS (
  SELECT DISTINCT country_code
    , city_id
    , vendor_id
  FROM deliveries
  WHERE DATE(rider_dropped_off_at, timezone) BETWEEN (SELECT start_time_shapes FROM parameters) AND (SELECT end_time FROM parameters)
  --- Take all distinct vendors which have completed at least one order in the past 4 weeks
), vendor_shapes AS (
  SELECT v.country_code
    , v.city_id
    , da.city_shape ------ Assign to each distinct vendor their last updated shape (over the whole time frame)
  FROM active_delivery_areas v
  LEFT JOIN rl.scorecard_infra_prep da ON v.country_code = da.country_code
    AND v.city_id = da.city_id
), alternative_city_shapes AS (
  SELECT country_code
    , ci.id AS city_id
    , ST_UNION_AGG(z.shape) AS city_geo
  --- If no shape is available in Porygon for a city, use the union of zone shapes found in cl.countries
  FROM cl.countries co
  LEFT JOIN UNNEST(cities) ci
  LEFT JOIN UNNEST(zones) z
  GROUP BY 1, 2
), final_shapes AS (
  SELECT z.country_code
    , z.city_id
    , ST_AREA(COALESCE(z.city_shape, ci.city_geo))/1000000 AS area ----- For all cities who have not migrated to Porygon, fill the nulls with ml.geo_api_city
  FROM vendor_shapes z
  LEFT JOIN alternative_city_shapes ci ON z.country_code = ci.country_code
    AND z.city_id = ci.city_id
-------------------- ORDER DENSITY --------------------
), orders_made AS (
  SELECT d.country_code
    , d.city_id
    , FORMAT_DATE('%G-%V', d.day) AS report_week
    , COUNT(DISTINCT del.order_id) AS orders
  FROM day_sequence d
  LEFT JOIN deliveries del ON d.country_code = del.country_code
    AND d.city_id = del.city_id
    AND DATE(DATETIME(rider_dropped_off_at, timezone)) = d.day
  GROUP BY 1, 2, 3
), densities AS (
  SELECT DISTINCT o.country_code
    , o.city_id
    , o.report_week AS report_week_local
    , ROUND(SAFE_DIVIDE(c.vendor_count, a.area), 2) AS vendor_density
    , ROUND(SAFE_DIVIDE(o.orders, a.area), 2) AS order_density
  FROM orders_made o
  LEFT JOIN active_vendors c ON o.country_code = c.country_code
    AND o.city_id = c.city_id
    AND o.report_week = c.report_week
  LEFT JOIN final_shapes a ON o.country_code = a.country_code
    AND o.city_id = a.city_id
---------------------------- GPS ACCURACY ----------------------------
), delivery_state AS (
  SELECT DISTINCT d.country_code
    , d.city_id
    , d.delivery_id
    , FORMAT_DATE('%G-%V', DATE(d.rider_dropped_off_at, d.timezone)) AS report_week_local
    , d.state
    , d.auto_transition
  FROM deliveries d
), gps_accuracy AS (
  SELECT d.country_code
    , d.city_id
    , report_week_local
    , COUNTIF(state = 'near_pickup' AND auto_transition IS TRUE) / COUNT(DISTINCT d.delivery_id) AS pickup_accuracy
    , COUNTIF(state = 'near_dropoff' AND auto_transition IS TRUE) / COUNT(DISTINCT d.delivery_id) AS dropoff_accuracy
  FROM delivery_state d
  GROUP BY 1, 2, 3
)
-------------------- FINAL AGGREGATION --------------------
SELECT d.country_code
  , d.city_id
  , d.report_week_local
  , de.vendor_density AS vendor_density
  , de.order_density AS order_density
  , d.pickup_distance AS pickup_distance
  , d.dropoff_distance AS dropoff_distance
  , g.dropoff_accuracy
  , g.pickup_accuracy
FROM distances d
LEFT JOIN densities de ON d.country_code = de.country_code
  AND d.city_id = de.city_id
  AND d.report_week_local = de.report_week_local
LEFT JOIN gps_accuracy g ON d.country_code = g.country_code
  AND d.city_id = g.city_id
  AND d.report_week_local = g.report_week_local
