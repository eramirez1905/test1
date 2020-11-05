CREATE OR REPLACE TABLE rl.scorecard_infra_prep AS
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
  GROUP BY 1,2,3
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
  GROUP BY 1,2,3
), delivery_area_prep AS (
  SELECT country_code
    , city_id
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
)
SELECT country_code
  , city_id
  , ST_UNION_AGG(shape) AS city_shape
FROM delivery_area_prep
WHERE DATE(active_to) >= (SELECT start_time_shapes FROM parameters)
GROUP BY 1,2
