CREATE OR REPLACE TABLE rl.scorecard_infra_weights_prep AS
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
    , o.vendor.id AS vendor_id
    , o.vendor.vendor_code
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
  GROUP BY 1,2,3
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
  GROUP BY 1, 2, 3, 4
), delivery_area_prep AS (
  SELECT country_code
    , city_id
    , vendor_id
    , CASE
        WHEN d.is_deleted IS FALSE AND h.created_at = h.updated_at THEN CURRENT_TIMESTAMP()
        ELSE active_to
      END AS active_to
    , shape
  FROM cl.vendors v
  LEFT JOIN UNNEST(v.delivery_areas) d
  LEFT JOIN UNNEST(d.history) h
  WHERE ('platform_delivery' IN UNNEST(delivery_provider)
      OR 'Hurrier' IN UNNEST(delivery_provider)
      )
    AND d.is_deleted IS FALSE
    AND operation_type NOT IN ('cancelled')
)
SELECT country_code
  , city_id
  , ST_UNION_AGG(shape) AS shape
FROM delivery_area_prep
WHERE DATE(active_to) >= (SELECT start_time_shapes FROM parameters)
GROUP BY 1,2
