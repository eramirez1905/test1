CREATE TEMPORARY FUNCTION is_assumed_actual_preparation_time(o ANY TYPE, d ANY TYPE, auto_transition ANY TYPE)
RETURNS BOOLEAN AS (
  o.sent_to_vendor_at IS NOT NULL
  -- rider late
  AND COALESCE(TIMESTAMP_DIFF(d.rider_near_restaurant_at, o.original_scheduled_pickup_at, SECOND), 0) <= 900
  -- rider_near_restaurant_at + buffer 30 secs < rider_picked_up_at
  AND TIMESTAMP_ADD(d.rider_near_restaurant_at, INTERVAL 30 SECOND) < d.rider_picked_up_at
  -- rider_near_restaurant_at + buffer 30 secs < rider_left_vendor_at
  AND TIMESTAMP_ADD(d.rider_near_restaurant_at, INTERVAL 30 SECOND) < d.rider_left_vendor_at
  -- rider_left_vendor_at + buffer 2 mins < rider_dropped_off_at
  AND TIMESTAMP_ADD(d.rider_left_vendor_at, INTERVAL 120 SECOND) < d.rider_dropped_off_at
  -- lef_pickup_time between 0.5 and 10 mins
  AND TIMESTAMP_DIFF(d.rider_left_vendor_at, d.rider_picked_up_at, SECOND) BETWEEN 30 AND 600
  -- courier_waiting_time between 0.5 and 25 mins
  AND GREATEST(COALESCE(TIMESTAMP_DIFF(d.rider_picked_up_at, d.rider_near_restaurant_at, SECOND), 0), 0) BETWEEN 30 AND 1500
  -- restaurant_time between 0.5 and 20 mins
  AND TIMESTAMP_DIFF(d.rider_left_vendor_at, d.rider_near_restaurant_at, SECOND) > 30 AND TIMESTAMP_DIFF(d.rider_left_vendor_at, d.rider_near_restaurant_at, SECOND) < 1200
  -- realized prep_time between 3 and 60 mins
  AND TIMESTAMP_DIFF(d.rider_picked_up_at, o.sent_to_vendor_at, SECOND) BETWEEN 180 AND 3600
  -- auto transition
  AND auto_transition.pickup IS NULL
  -- preorder
  AND o.is_preorder IS FALSE
);

CREATE TEMPORARY FUNCTION calculate_assumed_actual_preparation_time(rider_near_restaurant_at TIMESTAMP,
  rider_picked_up_at TIMESTAMP,
  sent_to_vendor_at TIMESTAMP,
  rider_left_vendor_at TIMESTAMP,
  o ANY TYPE)
RETURNS INT64 AS (
  CASE
    WHEN COALESCE(TIMESTAMP_DIFF(rider_near_restaurant_at, o.original_scheduled_pickup_at, SECOND), 0) <= 0
      THEN TIMESTAMP_DIFF(rider_picked_up_at, sent_to_vendor_at, SECOND)
    WHEN COALESCE(TIMESTAMP_DIFF(rider_near_restaurant_at, o.original_scheduled_pickup_at, SECOND), 0) > 0
        AND TIMESTAMP_DIFF(rider_left_vendor_at, rider_near_restaurant_at, SECOND) <= 300 + (o.vendor.walk_in_time * 2)
      THEN o.estimated_prep_time
    WHEN COALESCE(TIMESTAMP_DIFF(rider_near_restaurant_at, o.original_scheduled_pickup_at, SECOND), 0) > 0
        AND TIMESTAMP_DIFF(rider_left_vendor_at, rider_near_restaurant_at, SECOND) > 300 + (o.vendor.walk_in_time * 2)
      THEN TIMESTAMP_DIFF(rider_picked_up_at, sent_to_vendor_at, SECOND)
    ELSE NULL
  END
);

CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.orders`
PARTITION BY created_date
CLUSTER BY country_code, order_id, order_status AS
WITH order_cancelled_transition AS (
  SELECT *
  FROM `{{ params.project_id }}.cl._order_transitions`
  WHERE most_recent IS TRUE
    AND to_state = 'cancelled'
), vendors AS (
  SELECT h.id as vendor_id
        , h.country_code
        , v.vendor_code
        , h.name AS vendor_name
        , CAST(NULL AS INT64) AS walk_in_time
        , CAST(NULL AS INT64) AS walk_out_time
        , v.location
        , v.vertical_type
  FROM `{{ params.project_id }}.cl.vendors_v2` v
  LEFT JOIN UNNEST (hurrier) h
), orders AS (
  SELECT o.*
    , STRUCT(v.vendor_id AS id
        , v.vendor_code
        , v.vendor_name AS name
        , v.walk_in_time
        , v.walk_out_time
        , v.location
        , v.vertical_type
      ) AS vendor
  FROM `{{ params.project_id }}.cl._orders` o
  LEFT JOIN vendors v ON v.country_code = o.country_code
    AND v.vendor_id = o.vendor_id
), aggregated_deliveries AS (
  SELECT o.country_code
    , o.order_id
    , ARRAY_AGG(
        STRUCT(
          d.id
          , d.created_at
          , d.is_primary
          , d.is_redelivery
          , d.rider_id
          , d.city_id
          , d.timezone
          , d.delivery_status
          , d.delivery_reason
          , d.delivery_distance
          , d.rider_dispatched_at
          , d.rider_notified_at
          , d.rider_accepted_at
          , d.rider_near_restaurant_at
          , d.rider_picked_up_at
          , d.rider_near_customer_at
          , d.rider_dropped_off_at
          , d.auto_transition
          , d.pickup
          , d.dropoff
          , d.accepted
          , d.rider_starting_point_id
          , d.pickup_distance_manhattan
          , d.pickup_distance_google
          , d.pickup_distance_dte
          , d.dropoff_distance_dte
          , d.estimated_distance_and_time_dte
          , d.dropoff_distance_manhattan
          , d.dropoff_distance_google
          , d.transitions
          , d.vehicle
          , d.stacked_deliveries
          , d.is_stacked_intravendor
          , d.stacked_deliveries_details
          , STRUCT(
              COALESCE(TIMESTAMP_DIFF(rider_notified_at, first_rider_dispatched_at, SECOND), 0) AS dispatching_time
              -- condition to filter out orders that are cancelled and not accepted or the notified happened after acceptance
              , IF((d.delivery_status = 'cancelled' AND rider_accepted_at IS NULL) OR rider_accepted_at < rider_notified_at, NULL, TIMESTAMP_DIFF(rider_accepted_at, rider_notified_at, SECOND)) AS rider_reaction_time
              , COALESCE(TIMESTAMP_DIFF(rider_accepted_at, first_rider_dispatched_at, SECOND), 0) AS rider_accepting_time
              , IF(auto_transition.pickup IS NULL, COALESCE(TIMESTAMP_DIFF(rider_near_restaurant_at, rider_accepted_at, SECOND), 0), NULL) AS to_vendor_time
              , IF(auto_transition.pickup IS NULL, COALESCE(TIMESTAMP_DIFF(rider_left_vendor_at, rider_picked_up_at, SECOND), 0), NULL) AS vendor_arriving_time
              , IF(auto_transition.pickup IS NULL, COALESCE(TIMESTAMP_DIFF(rider_left_vendor_at, rider_picked_up_at, SECOND), 0), NULL) AS vendor_leaving_time
              , IF(auto_transition.pickup IS NULL, TIMESTAMP_DIFF(rider_near_restaurant_at, o.original_scheduled_pickup_at, SECOND), NULL) AS rider_late
              , IF(auto_transition.pickup IS NULL,
                  CASE
                    WHEN TIMESTAMP_DIFF(rider_near_restaurant_at, o.original_scheduled_pickup_at, SECOND) <= 0
                        AND TIMESTAMP_DIFF(rider_picked_up_at, o.original_scheduled_pickup_at, SECOND) <= 0
                      THEN 0
                    WHEN TIMESTAMP_DIFF(rider_near_restaurant_at, o.original_scheduled_pickup_at, SECOND) <= 0
                        AND TIMESTAMP_DIFF(rider_picked_up_at, o.original_scheduled_pickup_at, SECOND) > 0
                      THEN TIMESTAMP_DIFF(rider_picked_up_at, o.original_scheduled_pickup_at, SECOND)
                    WHEN TIMESTAMP_DIFF(rider_near_restaurant_at, o.original_scheduled_pickup_at, SECOND) BETWEEN 0 AND 600
                        AND TIMESTAMP_DIFF(rider_picked_up_at, rider_near_restaurant_at, SECOND) >= 300
                      THEN  TIMESTAMP_DIFF(rider_picked_up_at, o.original_scheduled_pickup_at, SECOND)
                    WHEN TIMESTAMP_DIFF(rider_near_restaurant_at, o.original_scheduled_pickup_at, SECOND) BETWEEN 0 AND 600
                        AND TIMESTAMP_DIFF(rider_picked_up_at, rider_near_restaurant_at, SECOND) < 300
                      THEN GREATEST(TIMESTAMP_DIFF(rider_picked_up_at, rider_near_restaurant_at, SECOND) , 0)
                    WHEN TIMESTAMP_DIFF(rider_near_restaurant_at, o.original_scheduled_pickup_at, SECOND) > 600
                      THEN NULL
                  END,
                  NULL
                ) AS vendor_late
              , IF(is_assumed_actual_preparation_time(o, d, auto_transition),
                  calculate_assumed_actual_preparation_time(rider_near_restaurant_at, rider_picked_up_at, sent_to_vendor_at, rider_left_vendor_at, o),
                  NULL
                ) AS assumed_actual_preparation_time
              , CASE
                  WHEN auto_transition.pickup IS NULL AND auto_transition.dropoff IS NULL
                      AND TIMESTAMP_DIFF(rider_near_restaurant_at, o.original_scheduled_pickup_at, SECOND) <= 0
                    THEN TIMESTAMP_DIFF(rider_dropped_off_at, rider_picked_up_at, SECOND)
                  WHEN auto_transition.pickup IS NULL AND auto_transition.dropoff IS NULL
                      AND TIMESTAMP_DIFF(rider_near_restaurant_at, o.original_scheduled_pickup_at, SECOND) > 0
                      AND TIMESTAMP_DIFF(rider_picked_up_at, rider_near_restaurant_at, SECOND) < 300
                    THEN TIMESTAMP_DIFF(rider_dropped_off_at, o.updated_scheduled_pickup_at, SECOND)
                  WHEN auto_transition.pickup IS NULL AND auto_transition.dropoff IS NULL
                      AND TIMESTAMP_DIFF(rider_near_restaurant_at, o.original_scheduled_pickup_at, SECOND) > 0
                      AND TIMESTAMP_DIFF(rider_picked_up_at, rider_near_restaurant_at, SECOND) >= 300
                  THEN TIMESTAMP_DIFF(rider_dropped_off_at, rider_picked_up_at, SECOND)
                  ELSE NULL
                END AS bag_time
              , IF(auto_transition.pickup IS NULL, GREATEST(COALESCE(TIMESTAMP_DIFF(rider_picked_up_at, rider_near_restaurant_at, SECOND), 0), 0), NULL) AS at_vendor_time
              , IF(auto_transition.pickup IS NULL, CASE
                  WHEN COALESCE(TIMESTAMP_DIFF(rider_near_restaurant_at, o.original_scheduled_pickup_at, SECOND), 0) BETWEEN 0 AND 600
                    THEN GREATEST(COALESCE(TIMESTAMP_DIFF(rider_picked_up_at, rider_near_restaurant_at, SECOND), 0), 0)
                  WHEN COALESCE(TIMESTAMP_DIFF(rider_near_restaurant_at, o.original_scheduled_pickup_at, SECOND), 0) < 0
                      AND (GREATEST(COALESCE(TIMESTAMP_DIFF(rider_picked_up_at, rider_near_restaurant_at, SECOND), 0), 0) + COALESCE(TIMESTAMP_DIFF(rider_near_restaurant_at, o.original_scheduled_pickup_at, SECOND), 0)) < 0
                    THEN 0
                  WHEN COALESCE(TIMESTAMP_DIFF(rider_near_restaurant_at, o.original_scheduled_pickup_at, SECOND), 0) < 0
                      AND (GREATEST(COALESCE(TIMESTAMP_DIFF(rider_picked_up_at, rider_near_restaurant_at, SECOND), 0), 0) + COALESCE(TIMESTAMP_DIFF(rider_near_restaurant_at, o.original_scheduled_pickup_at, SECOND), 0)) > 0
                    THEN GREATEST(COALESCE(TIMESTAMP_DIFF(rider_picked_up_at, rider_near_restaurant_at, SECOND), 0), 0) + COALESCE(TIMESTAMP_DIFF(rider_near_restaurant_at, o.original_scheduled_pickup_at, SECOND), 0)
                  ELSE NULL
                END, NULL) AS at_vendor_time_cleaned
              , IF(auto_transition.dropoff IS NULL, COALESCE(TIMESTAMP_DIFF(rider_near_customer_at, rider_picked_up_at, SECOND), 0), NULL) AS to_customer_time
              , CAST(dropoff_duration / 2 AS INT64) AS customer_walk_in_time
              , IF(auto_transition.dropoff IS NULL, GREATEST(COALESCE(TIMESTAMP_DIFF(rider_dropped_off_at, rider_near_customer_at, SECOND), 0), 0), NULL) AS at_customer_time
              , CAST(dropoff_duration / 2 AS INT64) AS customer_walk_out_time
              , COALESCE(TIMESTAMP_DIFF(rider_near_customer_at, o.created_at, SECOND), 0) AS actual_delivery_time
              , IF(auto_transition.dropoff IS NULL, TIMESTAMP_DIFF(rider_near_customer_at, o.created_at, SECOND) - TIMESTAMP_DIFF(o.promised_delivery_time, o.order_placed_at, SECOND), NULL) AS delivery_delay
            ) AS timings
        ) ORDER BY is_primary, d.created_at
    ) AS deliveries
  FROM orders o
  LEFT JOIN `{{ params.project_id }}.cl._deliveries` ds ON ds.country_code = o.country_code
    AND ds.order_id = o.order_id
  LEFT JOIN UNNEST(ds.deliveries) d
  GROUP BY 1,2
)
SELECT o.country_code
  , o.region
  , o.order_id
  , o.platform_order_id
  , o.platform_order_code
  , o.platform
  , o.entity
  , o.created_date
  , o.created_at
  , o.order_placed_at
  , o.city_id
  , oz.zone_id
  , o.timezone
  , o.is_preorder
  , o.estimated_prep_time
  , o.estimated_prep_buffer
  , o.cod_change_for
  , o.cod_collect_at_dropoff
  , o.cod_pay_at_pickup
  , o.delivery_fee
  , o.dropoff_address_id
  , o.online_tip
  , o.vendor_accepted_at
  , o.sent_to_vendor_at
  , o.original_scheduled_pickup_at
  , o.pickup_address_id
  , o.promised_delivery_time
  , o.updated_scheduled_pickup_at
  , o.stacking_group
  , o.order_status
  , o.tags
  , o.order_value
  , o.capacity
  , o.vendor_order_number
  , o.customer
  , o.vendor
  , p.porygon
  , ad.deliveries
  , STRUCT(
      IF(oct.performed_by = 'unn', 'api', oct.performed_by) AS `source`
      , oct.user_id AS performed_by
      , oct.reason AS reason
    ) AS cancellation
  , (
      SELECT AS STRUCT
        COALESCE(o.estimated_prep_time + TIMESTAMP_DIFF(o.updated_scheduled_pickup_at, o.original_scheduled_pickup_at, SECOND), 0) AS updated_prep_time
        , COALESCE(TIMESTAMP_DIFF(o.sent_to_vendor_at, o.created_at, SECOND), 0) AS hold_back_time
        , COALESCE(TIMESTAMP_DIFF(o.vendor_accepted_at, o.sent_to_vendor_at, SECOND), 0) AS vendor_reaction_time
        , delivery.timings.dispatching_time
        , delivery.timings.rider_reaction_time
        , delivery.timings.rider_accepting_time
        , delivery.timings.to_vendor_time
        , o.vendor.walk_in_time AS expected_vendor_walk_in_time
        , o.estimated_walk_in_duration
        , o.estimated_walk_out_duration
        , o.estimated_courier_delay
        , o.estimated_driving_time
        , delivery.timings.rider_late
        , delivery.timings.vendor_late
        , delivery.timings.assumed_actual_preparation_time
        , delivery.timings.bag_time
        , delivery.timings.at_vendor_time
        , delivery.timings.at_vendor_time_cleaned
        , delivery.timings.vendor_arriving_time
        , delivery.timings.vendor_leaving_time
        , o.vendor.walk_out_time AS expected_vendor_walk_out_time
        , delivery.timings.to_customer_time
        , delivery.timings.customer_walk_in_time
        , delivery.timings.at_customer_time
        , delivery.timings.customer_walk_out_time
        , delivery.timings.actual_delivery_time
        , COALESCE(TIMESTAMP_DIFF(o.promised_delivery_time, o.order_placed_at, SECOND), 0) AS promised_delivery_time
        , delivery.timings.delivery_delay AS order_delay
        , (SELECT AS STRUCT stats.mean_delay FROM f.stats ORDER BY created_at DESC LIMIT 1) AS zone_stats
      FROM UNNEST(ad.deliveries) delivery
      WHERE is_primary IS TRUE
    ) AS timings
FROM orders o
LEFT JOIN order_cancelled_transition oct ON oct.country_code = o.country_code
  AND oct.order_id = o.order_id
LEFT JOIN aggregated_deliveries ad ON ad.country_code = o.country_code
  AND ad.order_id = o.order_id
LEFT JOIN `{{ params.project_id }}.cl._orders_to_zones` oz ON oz.country_code = o.country_code
  AND oz.order_id = o.order_id
LEFT JOIN `{{ params.project_id }}.cl._porygon_drive_time` p ON o.country_code = p.country_code
  AND o.order_id = p.order_id
LEFT JOIN `{{ params.project_id }}.cl.zone_stats` f ON o.country_code = f.country_code
  AND oz.zone_id = f.zone_id
  AND TIMESTAMP_TRUNC(o.created_at, MINUTE) = f.created_at_bucket                                                                                                                                              
