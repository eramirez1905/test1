CREATE OR REPLACE TABLE rl.scorecard_infra_weights
PARTITION BY report_date_local AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 6 MONTH) AS start_time
    , DATE_SUB('{{ next_ds }}', INTERVAL 4 WEEK) AS start_time_shapes
    , DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY) AS end_time
), deliveries AS (
  SELECT o.country_code
    , d.city_id
    , o.order_id
    , o.is_preorder
    , o.order_status
    , o.timezone
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
    AND COALESCE(CAST(DATETIME(d.rider_dropped_off_at, d.timezone) AS DATE), CAST(DATETIME(d.created_at, d.timezone) AS DATE)) BETWEEN (SELECT start_time FROM parameters) AND (SELECT end_time FROM parameters)
), distances AS (
-------------------- Pickup and dropoff distance --------------------
  SELECT d.country_code
    , d.city_id
    , COALESCE(CAST(DATETIME(d.rider_dropped_off_at, d.timezone) AS DATE), CAST(DATETIME(d.created_at, d.timezone) AS DATE)) AS report_date_local
    , AVG(pickup_distance) AS pickup_distance
    , AVG(dropoff_distance) AS dropoff_distance
  FROM deliveries d
  WHERE delivery_status = 'completed'
  GROUP BY 1, 2, 3
), days AS (
-------------------- vendor density --------------------
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
  SELECT d.country_code
    , d.city_id
    , d.day
    , del.vendor_id
    , COUNT(DISTINCT del.order_id) AS orders_made
  FROM day_sequence d
  LEFT JOIN deliveries del ON d.country_code = del.country_code
    AND d.city_id = del.city_id
    AND DATE(DATETIME(rider_dropped_off_at, del.timezone)) BETWEEN DATE_SUB(d.day, INTERVAL 30 DAY) AND d.day
  WHERE delivery_status = 'completed'
  GROUP BY 1,2,3,4
), active_vendors AS (
  SELECT op.country_code
    , op.city_id
    , op.day
    , COUNT(DISTINCT op.vendor_id) AS count_vendor
  FROM vendors op
  LEFT JOIN rl.scorecard_infra_weights_prep da ON op.country_code = da.country_code
    AND op.city_id = da.city_id
  WHERE op.orders_made >= 1 ------ Take all vendors who completed at least 1 order during the last 30 days before the reference date
  GROUP BY 1,2,3
), vendor_shapes AS (
  SELECT op.country_code
    , op.city_id
    , op.day
    , da.shape AS city_shape
    , op.count_vendor
  FROM active_vendors op
  LEFT JOIN rl.scorecard_infra_weights_prep da ON op.country_code = da.country_code
    AND op.city_id = da.city_id
), density_porygon AS (
  SELECT d.country_code
    , d.city_id
    , d.day AS report_date
    , COALESCE(a.count_vendor, MIN(a.count_vendor) OVER(PARTITION BY d.country_code, d.city_id)) AS count_vendor
    , city_shape ---- for all countries who have migrated into Porygon, fill nulls with latest value
  FROM day_sequence d
  LEFT JOIN vendor_shapes a ON d.country_code = a.country_code
    AND d.city_id = a.city_id
    AND d.day = a.day
),vendor_density_all AS (
  SELECT dp.country_code
    , dp.city_id
    , dp.report_date
    , ST_AREA(COALESCE(dp.city_shape, ci.city_geog))/1000000 AS area ----- for all cities who have not migrated to Porygon, fill the nulls with lml.geo_api_city
    , dp.count_vendor
  FROM density_porygon dp
  LEFT JOIN (
    SELECT country_code
      , ci.id AS city_id
      , ST_UNION_AGG(SAFE.ST_GEOGFROMTEXT(z.zone_shape.wkt)) AS city_geog
    FROM cl.countries co
    LEFT JOIN UNNEST(cities) ci
    LEFT JOIN UNNEST(zones) z
    GROUP BY 1, 2
  ) ci USING(country_code, city_id)
), vendor_density AS (
  SELECT DISTINCT country_code
    , city_id
    , report_date AS report_date_local ----- not set to local time, using the suffix for naming purposes
    , ROUND(CAST((count_vendor / area) AS FLOAT64), 2) AS vendor_density
  FROM vendor_density_all
-------------------- ORDER DENSITY --------------------
), orders_made AS (
  SELECT d.country_code
    , d.city_id
    , d.day AS report_date
    , COUNT(DISTINCT del.order_id) AS orders
  FROM day_sequence d
  LEFT JOIN deliveries del ON d.country_code = del.country_code
    AND d.city_id = del.city_id
    AND DATE(DATETIME(rider_dropped_off_at, timezone)) = d.day
  WHERE order_status = 'completed'
  GROUP BY 1, 2, 3
), order_density AS (
  SELECT DISTINCT o.country_code
    , o.city_id
    , o.report_date AS report_date_local ----- not set to local time, using the suffix for naming purposes
    , ROUND(CAST((o.orders / v.area) AS FLOAT64), 2) AS order_density
  FROM orders_made o
  LEFT JOIN vendor_density_all v USING(country_code, city_id, report_date)
  WHERE o.orders IS NOT NULL
---------------------------- GPS ACCURACY ----------------------------
), delivery_state AS (
  SELECT DISTINCT d.country_code
    , d.city_id
    , d.delivery_id
    , CAST(DATETIME(d.rider_dropped_off_at, d.timezone) AS DATE) AS report_date_local
    , d.state
    , d.auto_transition
  FROM deliveries d
  WHERE d.delivery_status = 'completed'
), gps_accuracy AS (
  SELECT d.country_code
    , d.city_id
    , report_date_local
    , COUNTIF(state = 'near_pickup' AND auto_transition IS TRUE) / COUNT(DISTINCT d.delivery_id) AS pickup_accuracy
    , COUNTIF(state = 'near_dropoff' AND auto_transition IS TRUE) / COUNT(DISTINCT d.delivery_id) AS dropoff_accuracy
  FROM delivery_state d
  GROUP BY 1, 2, 3
------------------------------ UTR --------------------------------
), utr AS (
  SELECT country_code
    , d.city_id
    , COALESCE(DATE(DATETIME(d.rider_dropped_off_at, o.timezone)), DATE(DATETIME(d.created_at, o.timezone))) AS report_date
    , IF(d.delivery_status = 'completed', o.order_id, NULL) AS order_completed
    , NULL AS working_hours
  FROM cl.orders o
  LEFT JOIN UNNEST(deliveries) d
  WHERE o.created_date >= (SELECT start_time FROM parameters)

  UNION ALL

  SELECT country_code
    , city_id
    , e.day AS report_date
    , NULL AS order_completed
    , (e.duration / 3600) AS working_hours
  FROM cl.shifts s
  LEFT JOIN UNNEST(s.actual_working_time_by_date) e
  WHERE s.created_date >= (SELECT start_time FROM parameters)
), utr_agg AS (
  SELECT country_code
    , city_id
    , report_date AS report_date_local
    , COUNT(DISTINCT order_completed) AS orders_completed
    , SUM(working_hours) AS working_hours
  FROM utr
  GROUP BY 1, 2, 3
)
-------------------- FINAL AGGREGATION --------------------
SELECT u.country_code
  , u.city_id
  , u.report_date_local
  , SAFE_DIVIDE(orders_completed, working_hours) AS utr
  , v.vendor_density AS vendor_density
  , o.order_density AS order_density
  , d.pickup_distance AS pickup_distance
  , d.dropoff_distance AS dropoff_distance
  , dropoff_accuracy
  , pickup_accuracy
FROM utr_agg u
LEFT JOIN distances d USING(country_code, city_id, report_date_local)
LEFT JOIN vendor_density v USING(country_code, city_id, report_date_local)
LEFT JOIN order_density o USING(country_code, city_id, report_date_local)
LEFT JOIN gps_accuracy USING(country_code, city_id, report_date_local)
