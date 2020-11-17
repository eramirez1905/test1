CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.sanity_checks_deliveries` AS
WITH dataset AS (
  SELECT COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone)) AS report_date
    , o.country_code
    , 1 - (COUNT(d.pickup_distance_google) / COUNT(d.id)) AS perc_error_pickup_distance_google
    , 1 - (COUNT(d.pickup_distance_manhattan) / COUNT(d.id)) AS perc_error_pickup_distance_manhattan
    , 1 - (COUNT(d.dropoff_distance_google) / COUNT(d.id)) AS perc_error_dropoff_distance_google
    , 1 - (COUNT(d.dropoff_distance_manhattan) / COUNT(d.id)) AS perc_error_dropoff_distance_manhattan
    , 1 - (COUNT(d.created_at) / COUNT(d.id)) AS perc_error_created_at
    , 1 - (COUNT(d.rider_accepted_at) / COUNT(d.id)) AS perc_error_rider_accepted_at
    , 1 - (COUNT(d.rider_notified_at) / COUNT(d.id)) AS perc_error_rider_notified_at
    , 1 - (COUNT(d.rider_id) / COUNT(d.id)) AS perc_error_rider_id
    , 1 - (COUNT(d.rider_near_customer_at) / COUNT(d.id)) AS perc_error_rider_near_customer_at
    , 1 - (COUNT(d.rider_near_restaurant_at) / COUNT(d.id)) AS perc_error_rider_near_restaurant_at
    , 1 - (COUNT(d.rider_picked_up_at) / COUNT(d.id)) AS perc_error_rider_picked_up_at
    , 1 - (COUNT(d.rider_dropped_off_at) / COUNT(d.id)) AS perc_error_rider_dropped_off_at
    , 1 - (COUNT(d.vehicle.vehicle_bag) / COUNT(d.id)) AS perc_error_rider_vehicle_bag
    , 1 - (COUNT(d.city_id) / COUNT(d.id)) AS perc_error_city_id
    , COUNT(IF(d.pickup_distance_manhattan > 25000, d.id, NULL)) / COUNT(d.id) AS pickup_distance_manhattan_over_25_km
    , COUNT(IF(d.dropoff_distance_manhattan > 25000, d.id, NULL)) / COUNT(d.id) AS dropoff_distance_manhattan_over_25_km
    , COUNT(IF(d.dropoff_distance_google > 25000, d.id, NULL)) / COUNT(d.id) AS dropoff_distance_google_over_25_km
    , COUNT(IF(d.pickup_distance_google > 25000, d.id, NULL)) / COUNT(d.id) AS pickup_distance_google_over_25_km
    , COUNT(DISTINCT(IF(o.timings.promised_delivery_time < 0, o.order_id, NULL))) negative_promised_delivery_time
  FROM `{{ params.project_id }}.cl.orders` o
  LEFT JOIN UNNEST (deliveries) d 
  WHERE o.created_date > DATE_SUB('{{ next_ds }}', INTERVAL 1 MONTH)
    AND d.delivery_status = 'completed'
  GROUP BY 1, 2
)
SELECT *
FROM dataset
WHERE report_date < '{{ next_ds }}'
