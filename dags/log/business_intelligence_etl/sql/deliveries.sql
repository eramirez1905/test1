CREATE OR REPLACE TABLE il.deliveries
PARTITION BY created_date
CLUSTER BY country_code, delivery_id, order_id AS
WITH assigned_riders AS (
  SELECT country_code
    , delivery_id
    , order_id
    -- this flag indicates if the rider picks up another order while already having 1 order
    , NOT (deliveries_in_bag_past = 1 AND deliveries_in_bag_future = 1) AS rider_stacked
    , (is_in_bag_intravendor_past OR is_in_bag_intravendor_future ) AS rider_stacked_same_vendor
  FROM `{{ params.project_id }}.cl.stacked_deliveries`
  WHERE NOT (deliveries_in_bag_past = 1 AND deliveries_in_bag_future = 1)
    AND created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 60 + 1 DAY)
), riders AS (
  SELECT country_code
    , rider_id
    , courier_id
  FROM `{{ params.project_id }}.cl.riders`
  LEFT JOIN UNNEST(hurrier_courier_ids) courier_id
), vehicles AS (
  SELECT d.country_code
   , d.id AS delivery_id
   , d.vehicle_type_id
   , d.vehicle_id
   , d.created_date
   , STRUCT(
       ve.id
       , ve.name
       , ve.profile
       , b.name AS bag_name
       , CONCAT(ve.name, ' - ', b.name) AS vehicle_bag
    ) AS vehicle
  FROM `{{ params.project_id }}.ml.hurrier_deliveries` d
  LEFT JOIN `{{ params.project_id }}.ml.hurrier_vehicle_types` ve ON ve.country_code = d.country_code
    AND ve.id = d.vehicle_type_id
  LEFT JOIN `{{ params.project_id }}.ml.hurrier_vehicles` v ON ve.country_code = v.country_code
    AND v.vehicle_type_id = ve.id
    AND v.id = d.vehicle_id
  LEFT JOIN `{{ params.project_id }}.ml.hurrier_vehicle_bag_types` b ON v.country_code = b.country_code
    AND v.vehicle_bag_type_id = b.id
    AND v.vehicle_type_id = d.vehicle_type_id
  WHERE d.created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 60 + 1 DAY)
), delivery_transitions AS (
  SELECT o.country_code
    , d.id AS delivery_id
    , t.created_at
    , t.auto_transition
    , t.state AS to_state
    , d.pickup_distance_manhattan AS pickup_distance
    , d.rider_starting_point_id AS dropoff_zone_id
  FROM `{{ params.project_id }}.cl.orders` o
  LEFT JOIN UNNEST(deliveries) d
  LEFT JOIN UNNEST(transitions) t
  WHERE o.created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 60 + 1 DAY)
), deliveries AS (
  SELECT d.country_code
    , d.id AS delivery_id
    , d.order_id
    , d.status AS delivery_status
    , d.redelivery
    , c.rider_id
    , ar.rider_stacked AS stacked_flag
    , ar.rider_stacked_same_vendor AS stacked_flag_intravendor
    , d.created_at
    , cast(d.created_at AS DATE) AS created_date
    , d.courier_dispatched_at AS rider_assigned
    , d.courier_accepted_at AS rider_accepted
    , d.courier_picked_up_at AS food_picked_up
    , d.courier_dropped_off_at AS food_delivered
    , d.dropoff_address_id
    , d.pickup_address_id
    , CAST(d.dropoff_duration / 2 AS INT64) AS customer_walk_in_time
    , CAST(d.dropoff_duration / 2 AS INT64) AS customer_walk_out_time
    , CASE
        WHEN (d.delivery_distance / 1000) > 25
          THEN NULL
        ELSE (d.delivery_distance / 1000)
      END AS delivery_distance
    , d.utilization
    , d.courier_avg_speed
    , CASE
      WHEN (d.distance_travelled_dropoff_google / 1000) > 25
        THEN NULL
        ELSE (d.distance_travelled_dropoff_google / 1000)
      END AS dropoff_distance_google
    , CASE
      WHEN (d.distance_travelled_pickup_google / 1000) > 25
        THEN NULL
        ELSE (d.distance_travelled_pickup_google / 1000)
      END AS pickup_distance_google
    , array((
        SELECT AS STRUCT
            created_at
          , auto_transition
          , to_state
          , pickup_distance
          , dropoff_zone_id
        FROM delivery_transitions dt
        WHERE dt.country_code = d.country_code
          AND dt.delivery_id = d.id
    )) AS transitions
    , v.vehicle
  FROM `{{ params.project_id }}.ml.hurrier_deliveries` d
  LEFT JOIN assigned_riders ar ON ar.country_code = d.country_code
    AND ar.delivery_id = d.id
  LEFT JOIN riders c ON c.country_code = d.country_code
    AND c.courier_id = d.courier_id
  LEFT JOIN vehicles v ON v.country_code = d.country_code
    AND v.delivery_id = d.id
  WHERE d.created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 60 DAY)
), pickup_distance_datatset AS (
  select country_code
    , delivery_id
    , max(t.pickup_distance / 1000) AS pickup_distance
  from deliveries d
    , UNNEST(transitions) t
  WHERE t.to_state = 'dispatched'
  GROUP BY 1,2
), final_data AS (
  SELECT d.*
    , ci.id AS city_id
    , ci.name AS city_name
    , ci.timezone
    , CASE
      WHEN COALESCE(t.pickup_distance / 1000, pd.pickup_distance) > 25
        THEN NULL
        ELSE COALESCE(t.pickup_distance / 1000, pd.pickup_distance)
      END AS pickup_distance
    , CASE
      WHEN (t.dropoff_distance / 1000) > 25
        THEN NULL
        ELSE (t.dropoff_distance / 1000)
      END AS dropoff_distance
    , TIMESTAMP_ADD(d.rider_accepted, INTERVAL t.to_pickup_time SECOND) AS rider_at_restaurant
    , TIMESTAMP_ADD(d.food_picked_up, INTERVAL t.to_dropoff_time SECOND) AS rider_at_customer
    , TIMESTAMP_DIFF(d.food_delivered, t.placed_at, MINUTE) AS delivery_time
    , t.dispatching_time / 60 AS dispatching_time
    , t.courier_reaction_time / 60 AS rider_reaction_time
    , t.accepting_time / 60 AS accepting_time
    , t.to_pickup_time / 60 AS driving_to_restaurant_time
    , CASE
        WHEN t.pickup_time <= 5
          THEN NULL
          ELSE (t.pickup_time / 60)
        END
      AS at_restaurant_time
    , CASE
        WHEN t.to_dropoff_time <= 5
          THEN NULL
          ELSE (t.to_dropoff_time / 60)
        END
      AS driving_to_customer_time
    , CASE
        WHEN t.dropoff_time <= 5
          THEN NULL
          ELSE (t.dropoff_time / 60)
        END
      AS at_customer_time
    , t.delivery_late / 60 AS delivery_delay
    , t.courier_late / 60 AS rider_late_time
    , t.time_in_bag / 60 AS bag_time
    , a.latitude AS customer_lat
    , a.longitude AS customer_lng
  FROM deliveries d
  LEFT JOIN `{{ params.project_id }}.ml.hurrier_delivery_timings` t ON t.country_code = d.country_code
    AND d.delivery_id = t.delivery_id
    AND t.created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 60 + 1 DAY)
  LEFT JOIN pickup_distance_datatset pd ON pd.country_code = d.country_code
    AND d.delivery_id = pd.delivery_id
  LEFT JOIN `{{ params.project_id }}.ml.hurrier_addresses` a ON d.dropoff_address_id = a.id
    AND a.country_code = d.country_code
  LEFT JOIN `{{ params.project_id }}.ml.hurrier_cities` ci ON a.city_id = ci.id
    AND ci.country_code = a.country_code
)
SELECT *
  , d.rider_accepted AS start_time
  , COALESCE(d.food_delivered, d.rider_at_customer, d.food_picked_up, d.rider_at_restaurant, d.rider_accepted) AS end_time
  , CAST(d.rider_accepted AS DATE) AS report_date
FROM final_data d
