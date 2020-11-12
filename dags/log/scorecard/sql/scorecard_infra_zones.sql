CREATE OR REPLACE TABLE rl.scorecard_infra_zones AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 10 WEEK) AS start_time
    , DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY) AS end_time
), deliveries_to_zones AS (
  SELECT o.country_code
    , d.city_id
    , o.zone_id
    , o.order_id
    , o.is_preorder
    , o.timezone
    , vendor.id AS vendor_id
    , vendor.vendor_code
    , d.id AS delivery_id
    , d.timings.actual_delivery_time
    , d.rider_dropped_off_at
    , COALESCE(d.pickup_distance_google, d.pickup_distance_manhattan) AS pickup_distance
    , COALESCE(d.dropoff_distance_google, d.dropoff_distance_manhattan) AS dropoff_distance
    , d.created_at
    , o.created_date
    , t.state
    , t.auto_transition
  FROM cl.orders o
  LEFT JOIN UNNEST(deliveries) d
  LEFT JOIN UNNEST(d.transitions) t
  WHERE o.created_date >= (SELECT start_time FROM parameters)
    AND CAST(DATETIME(d.rider_dropped_off_at, d.timezone) AS DATE) BETWEEN (SELECT start_time FROM parameters) AND (SELECT end_time FROM parameters)
    AND d.delivery_status = 'completed'
-------------------- VENDOR AND ORDER DENSITY --------------------
), days AS (
   SELECT GENERATE_DATE_ARRAY((SELECT start_time FROM parameters), (SELECT end_time FROM parameters), INTERVAL 1 DAY) AS day
), day_sequence AS (
    SELECT c.country_code
    , c.city_id
    , c.zone_id
    , day
  FROM (
    SELECT pc.country_code
      , ci.id AS city_id
      , z.id AS zone_id
    FROM cl.countries pc
    LEFT JOIN UNNEST(cities) ci
    LEFT JOIN UNNEST(zones) z
  ) c
  CROSS JOIN days
  LEFT JOIN UNNEST(days.day) AS day
), vendors AS (
  SELECT country_code
    , city_id
    , zone_id
    , vendor_id
    , ARRAY_AGG(DISTINCT FORMAT_DATE('%G-%V', CAST(DATETIME(rider_dropped_off_at, timezone) AS DATE))) AS active_weeks
    --- Assign to each vendor an array of weeks where this vendor has delivered at least one order
  FROM deliveries_to_zones
  GROUP BY 1, 2, 3, 4
), active_vendors AS (
  SELECT FORMAT_DATE('%G-%V', d.day) AS report_week
      , d.country_code
      , d.city_id
      , d.zone_id
      , COUNT(DISTINCT(IF(FORMAT_DATE('%G-%V', d.day) IN UNNEST(v.active_weeks), vendor_id, NULL))) AS vendor_count
      ---- Vendor are considered active if they have completed at least one order on the report week
  FROM day_sequence d
  LEFT JOIN vendors v ON d.country_code = v.country_code
    AND d.city_id = v.city_id
    AND d.zone_id = v.zone_id
  GROUP BY 1, 2, 3, 4
), orders_made AS (
  SELECT d.country_code
    , d.city_id
    , d.zone_id
    , FORMAT_DATE('%G-%V', DATE(DATETIME(rider_dropped_off_at, timezone))) AS report_week
    , COUNT(DISTINCT order_id) AS orders
  FROM deliveries_to_zones d
  GROUP BY 1, 2, 3, 4
), densities AS (
  SELECT DISTINCT o.country_code
    , o.city_id
    , o.zone_id
    , o.report_week AS report_week_local
    , ROUND(SAFE_DIVIDE(c.vendor_count, a.area), 2) AS vendor_density
    , ROUND(SAFE_DIVIDE(o.orders, a.area), 2) AS order_density
  FROM orders_made o
  LEFT JOIN active_vendors c ON o.country_code = c.country_code
    AND o.city_id = c.city_id
    AND o.zone_id = c.zone_id
    AND o.report_week = c.report_week
  LEFT JOIN rl.scorecard_infra_zones_shapes a ON o.country_code = a.country_code
    AND o.city_id = a.city_id
    AND o.zone_id = a.zone_id
-------------------- PICKUP AND DROP-OFF DISTANCES --------------------
), distances AS (
  SELECT country_code
    , city_id
    , zone_id
    , FORMAT_DATE('%G-%V', COALESCE(CAST(DATETIME(d.rider_dropped_off_at, d.timezone) AS DATE), CAST(DATETIME(d.created_at, d.timezone) AS DATE))) AS report_week_local
    , AVG(pickup_distance / 1000) AS pickup_distance
    , AVG(dropoff_distance / 1000) AS dropoff_distance
  FROM deliveries_to_zones d
  GROUP BY 1, 2, 3, 4
---------------------------- GPS ACCURACY ----------------------------
), delivery_state AS (
  SELECT DISTINCT country_code
    , city_id
    , zone_id
    , delivery_id
    , FORMAT_DATE('%G-%V', CAST(DATETIME(rider_dropped_off_at, timezone) AS DATE)) AS report_week_local
    , state
    , auto_transition
  FROM deliveries_to_zones d
), gps_accuracy AS (
  SELECT d.country_code
    , d.city_id
    , d.zone_id
    , report_week_local
    , COUNTIF(state = 'near_pickup' AND auto_transition IS TRUE) / COUNT(DISTINCT d.delivery_id) AS pickup_accuracy
    , COUNTIF(state = 'near_dropoff' AND auto_transition IS TRUE) / COUNT(DISTINCT d.delivery_id) AS dropoff_accuracy
  FROM delivery_state d
  GROUP BY 1, 2, 3, 4
)
-------------------- FINAL AGGREGATION --------------------
SELECT d.country_code
  , d.city_id
  , d.zone_id
  , d.report_week_local
  , v.vendor_density AS vendor_density
  , v.order_density AS order_density
  , d.pickup_distance AS pickup_distance
  , d.dropoff_distance AS dropoff_distance
  , dropoff_accuracy
  , pickup_accuracy
FROM distances d
LEFT JOIN densities v USING(country_code, city_id, zone_id, report_week_local)
LEFT JOIN gps_accuracy USING(country_code, city_id, zone_id, report_week_local)
WHERE d.country_code IS NOT NULL
