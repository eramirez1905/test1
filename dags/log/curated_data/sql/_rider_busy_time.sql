CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._rider_busy_time`
PARTITION BY working_day
CLUSTER BY country_code, state, rider_id, created_at AS
WITH busy_time AS (
  SELECT o.country_code
    , 'busy_started_at' AS state
    , d.rider_id
    , o.city_id
    , d.rider_starting_point_id AS starting_point_id
    , d.rider_accepted_at AS created_at
    , o.timezone
    , CAST(NULL AS TIMESTAMP) AS shift_started_at
    , CAST(NULL AS TIMESTAMP) AS shift_ended_at
    , o.order_id
    , d.id AS delivery_id
  FROM `{{ params.project_id }}.cl.orders` o
  LEFT JOIN UNNEST (deliveries) d
  WHERE d.delivery_status = 'completed'
  UNION ALL
  SELECT o.country_code
    , 'busy_ended_at' AS state
    , d.rider_id
    , o.city_id
    , d.rider_starting_point_id AS starting_point_id
    , COALESCE(d.rider_dropped_off_at, d.rider_near_customer_at, d.rider_picked_up_at, d.rider_near_restaurant_at, d.rider_accepted_at) AS created_at
    , o.timezone
    , CAST(NULL AS TIMESTAMP) AS shift_started_at
    , CAST(NULL AS TIMESTAMP) AS shift_ended_at
    , o.order_id
    , d.id AS delivery_id
  FROM `{{ params.project_id }}.cl.orders` o
  LEFT JOIN UNNEST (deliveries) d
  WHERE d.delivery_status = 'completed'
)
SELECT country_code
  , CAST(created_at AS DATE) AS working_day
  , state
  , rider_id
  , city_id
  , starting_point_id
  , NULL AS ready_id
  , created_at
  , shift_started_at
  , shift_ended_at
  , timezone
  , order_id
  , delivery_id
FROM busy_time
