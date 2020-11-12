WITH deliveries_agg_transitions AS (
  SELECT
    orders.country_code,
    deliveries.id AS delivery_id,
    ARRAY_AGG(
      STRUCT(
        transitions.state,
        transitions.auto_transition AS is_auto_transition,
        transitions.latest AS is_latest,
        transitions.first AS is_first,
        transitions.geo_point AS point_geo,
        transitions.dispatch_type,
        transitions.undispatch_type,
        transitions.event_type,
        transitions.update_reason,
        STRUCT(
          transitions.actions.user_id AS lg_user_id,
          `{project_id}`.pandata_intermediate.LG_UUID(transitions.actions.user_id, orders.country_code) AS lg_user_uuid,
          transitions.actions.performed_by AS performed_by_type,
          transitions.actions.issue_type,
          transitions.actions.reason,
          transitions.actions.comment
        ) AS actions,
        transitions.estimated_pickup_arrival_at AS estimated_pickup_arrival_at_utc,
        transitions.estimated_dropoff_arrival_at AS estimated_dropoff_arrival_at_utc,
        transitions.created_at AS created_at_utc
      )
    ) AS transitions
  FROM `fulfillment-dwh-production.curated_data_shared.orders` AS orders
  CROSS JOIN UNNEST (orders.deliveries) AS deliveries
  CROSS JOIN UNNEST (deliveries.transitions) AS transitions
  GROUP BY
    orders.country_code,
    deliveries.id
),

deliveries_agg_stacked_deliveries_details AS (
  SELECT
    orders.country_code,
    deliveries.id AS delivery_id,
    ARRAY_AGG(
      STRUCT(
        stacked_deliveries_details.delivery_id AS lg_delivery_id,
        `{project_id}`.pandata_intermediate.LG_UUID(stacked_deliveries_details.delivery_id, orders.country_code) AS lg_delivery_uuid
      )
    ) AS stacked_deliveries
  FROM `fulfillment-dwh-production.curated_data_shared.orders` AS orders
  CROSS JOIN UNNEST (orders.deliveries) AS deliveries
  CROSS JOIN UNNEST (deliveries.stacked_deliveries_details) AS stacked_deliveries_details
  GROUP BY
    orders.country_code,
    deliveries.id
),

orders_agg_deliveries AS (
  SELECT
    orders.country_code,
    orders.order_id,
    ARRAY_AGG(
      STRUCT(
        deliveries.id,
        `{project_id}`.pandata_intermediate.LG_UUID(deliveries.id, orders.country_code) AS uuid,
        deliveries.rider_id AS lg_rider_id,
        `{project_id}`.pandata_intermediate.LG_UUID(deliveries.rider_id, orders.country_code) AS lg_rider_uuid,
        deliveries.rider_starting_point_id AS lg_rider_starting_point_id,
        `{project_id}`.pandata_intermediate.LG_UUID(deliveries.rider_starting_point_id, orders.country_code) AS lg_rider_starting_point_uuid,

        deliveries.delivery_status AS status,
        deliveries.delivery_distance AS distance_in_meters,
        deliveries.stacked_deliveries AS stacked_deliveries_count,

        deliveries.auto_transition.pickup AS is_pickup_auto_transition,
        deliveries.auto_transition.dropoff AS is_dropoff_auto_transistion,
        deliveries.is_primary,
        deliveries.is_redelivery,
        deliveries.is_stacked_intravendor,

        deliveries.pickup AS pickup_geo,
        deliveries.dropoff AS dropoff_geo,
        deliveries.accepted AS accepted_geo,

        deliveries.pickup_distance_manhattan AS pickup_distance_manhattan_in_meters,
        deliveries.pickup_distance_google AS pickup_distance_google_in_meters,
        deliveries.dropoff_distance_manhattan AS dropoff_distance_manhattan_in_meters,
        deliveries.dropoff_distance_google AS dropoff_distance_google_in_meters,

        deliveries.timings.dispatching_time AS dispatching_time_in_seconds,
        deliveries.timings.rider_reaction_time AS rider_reaction_time_in_seconds,
        deliveries.timings.rider_accepting_time AS rider_accepting_time_in_seconds,
        deliveries.timings.to_vendor_time AS to_vendor_time_in_seconds,
        deliveries.timings.vendor_arriving_time AS vendor_arriving_time_in_seconds,
        deliveries.timings.vendor_leaving_time AS vendor_leaving_time_in_seconds,
        deliveries.timings.rider_late AS rider_late_in_seconds,
        deliveries.timings.vendor_late AS vendor_late_in_seconds,
        deliveries.timings.assumed_actual_preparation_time AS assumed_actual_preparation_time_in_seconds,
        deliveries.timings.bag_time AS bag_time_in_seconds,
        deliveries.timings.at_vendor_time AS at_vendor_time_in_seconds,
        deliveries.timings.at_vendor_time_cleaned AS at_vendor_time_cleaned_in_seconds,
        deliveries.timings.to_customer_time AS to_customer_time_in_seconds,
        deliveries.timings.customer_walk_in_time AS customer_walk_in_time_in_seconds,
        deliveries.timings.at_customer_time AS at_customer_time_in_seconds,
        deliveries.timings.customer_walk_out_time AS customer_walk_out_time_in_seconds,
        deliveries.timings.actual_delivery_time AS actual_delivery_time_in_seconds,
        deliveries.timings.delivery_delay AS delivery_delay_in_seconds,
        deliveries.rider_notified_at AS rider_notified_at_utc,
        deliveries.rider_dispatched_at AS rider_dispatched_at_utc,
        deliveries.rider_accepted_at AS rider_accepted_at_utc,
        deliveries.rider_near_restaurant_at AS rider_near_restaurant_at_utc,
        deliveries.rider_picked_up_at AS rider_picked_up_at_utc,
        deliveries.rider_near_customer_at AS rider_near_customer_at_utc,
        deliveries.created_at AS created_at_utc,

        deliveries.vehicle,
        deliveries_agg_stacked_deliveries_details.stacked_deliveries,
        deliveries_agg_transitions.transitions
      )
    ) AS deliveries,
  FROM `fulfillment-dwh-production.curated_data_shared.orders` AS orders
  CROSS JOIN UNNEST (orders.deliveries) AS deliveries
  LEFT JOIN deliveries_agg_transitions
         ON orders.country_code = deliveries_agg_transitions.country_code
        AND deliveries.id = deliveries_agg_transitions.delivery_id
  LEFT JOIN deliveries_agg_stacked_deliveries_details
         ON orders.country_code = deliveries_agg_stacked_deliveries_details.country_code
        AND deliveries.id = deliveries_agg_stacked_deliveries_details.delivery_id
  GROUP BY
    orders.country_code,
    orders.order_id
),

orders_agg_porygon AS (
  SELECT
    orders.country_code,
    orders.order_id,
    ARRAY_AGG(
      STRUCT(
        porygon.vehicle_profile,
        porygon.drive_time_value AS drive_time_in_minutes,
        porygon.active_vehicle_profile AS active_vehicle_profile
      )
    ) AS porygon
  FROM `fulfillment-dwh-production.curated_data_shared.orders` AS orders
  CROSS JOIN UNNEST (orders.porygon) AS porygon
  GROUP BY
    orders.country_code,
    orders.order_id
)

SELECT
  `{project_id}`.pandata_intermediate.LG_UUID(orders.order_id, orders.country_code) AS uuid,
  orders.order_id AS id,
  orders.platform_order_code AS code,
  `{project_id}`.pandata_intermediate.EXCLUDE_T2(orders.country_code) AS country_code,
  orders.region,
  `{project_id}`.pandata_intermediate.LG_UUID(
    `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(orders.country_code, orders.city_id),
    orders.country_code
  ) AS lg_city_uuid,
  `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(orders.country_code, orders.city_id) AS lg_city_id,
  `{project_id}`.pandata_intermediate.LG_UUID(orders.zone_id, orders.country_code) AS lg_zone_uuid,
  orders.zone_id AS lg_zone_id,
  `{project_id}`.pandata_intermediate.LG_UUID(orders.dropoff_address_id, orders.country_code) AS lg_dropoff_address_uuid,
  orders.dropoff_address_id AS lg_dropoff_address_id,
  `{project_id}`.pandata_intermediate.LG_UUID(orders.pickup_address_id, orders.country_code) AS lg_pickup_address_uuid,
  orders.pickup_address_id AS lg_pickup_address_id,

  orders.stacking_group,
  orders.is_preorder,

  orders.platform,
  orders.entity,
  orders.order_status,
  orders.tags,
  orders.capacity / 100 AS capacity,
  orders.vendor_order_number,
  orders.customer.location AS customer_location_geo,

  -- time
  orders.estimated_prep_time AS estimated_prep_time_in_minutes,
  orders.estimated_prep_buffer AS estimated_prep_buffer_in_minutes,
  orders.timings.updated_prep_time AS updated_prep_time_in_seconds,
  orders.timings.hold_back_time AS hold_back_time_in_seconds,
  orders.timings.vendor_reaction_time AS vendor_reaction_time_in_seconds,
  orders.timings.dispatching_time AS dispatching_time_in_seconds,
  orders.timings.rider_reaction_time AS rider_reaction_time_in_seconds,
  orders.timings.rider_accepting_time AS rider_accepting_time_in_seconds,
  orders.timings.to_vendor_time AS to_vendor_time_in_seconds,
  orders.timings.expected_vendor_walk_in_time AS expected_vendor_walk_in_time_in_seconds,
  orders.timings.estimated_walk_in_duration AS estimated_walk_in_duration_in_seconds,
  orders.timings.estimated_walk_out_duration AS estimated_walk_out_duration_in_seconds,
  orders.timings.estimated_courier_delay AS estimated_courier_delay_in_seconds,
  orders.timings.estimated_driving_time AS estimated_driving_time_in_seconds,
  orders.timings.rider_late AS rider_late_in_seconds,
  orders.timings.vendor_late AS vendor_late_in_seconds,
  orders.timings.assumed_actual_preparation_time AS assumed_actual_preparation_time_in_seconds,
  orders.timings.bag_time AS bag_time_in_seconds,
  orders.timings.at_vendor_time AS at_vendor_time_in_seconds,
  orders.timings.at_vendor_time_cleaned AS at_vendor_time_cleaned_in_seconds,
  orders.timings.vendor_arriving_time AS vendor_arriving_time_in_seconds,
  orders.timings.vendor_leaving_time AS vendor_leaving_time_in_seconds,
  orders.timings.expected_vendor_walk_out_time AS expected_vendor_walk_out_time_in_seconds,
  orders.timings.to_customer_time AS to_customer_time_in_seconds,
  orders.timings.customer_walk_in_time AS customer_walk_in_time_in_seconds,
  orders.timings.at_customer_time AS at_customer_time_in_seconds,
  orders.timings.customer_walk_out_time AS customer_walk_out_time_in_seconds,
  orders.timings.actual_delivery_time AS actual_delivery_time_in_seconds,
  orders.timings.promised_delivery_time AS promised_delivery_time_in_seconds,
  orders.timings.order_delay AS order_delay_in_seconds,
  orders.timings.zone_stats.mean_delay AS mean_delay_in_zone_in_seconds,

  -- currency
  orders.cod_change_for AS cod_change_for_local,
  orders.cod_collect_at_dropoff AS cod_collect_at_dropoff_local,
  orders.cod_pay_at_pickup AS cod_pay_at_pickup_local,
  orders.delivery_fee AS delivery_fee_local,
  orders.online_tip AS online_tip_local,
  orders.order_value AS total_value_local,

  -- timestamps
  orders.timezone,
  orders.updated_scheduled_pickup_at AS updated_scheduled_pickup_at_utc,
  orders.promised_delivery_time AS promised_delivery_at_utc,
  orders.vendor_accepted_at AS vendor_accepted_at_utc,
  orders.sent_to_vendor_at AS sent_to_vendor_at_utc,
  orders.original_scheduled_pickup_at AS original_scheduled_pickup_at_utc,
  orders.order_placed_at AS order_placed_at_utc,
  orders.created_date AS created_date_utc,
  orders.created_at AS created_at_utc,
  orders_agg_deliveries.deliveries,
  orders_agg_porygon.porygon,
  STRUCT(
    orders.cancellation.performed_by AS performed_by_lg_user_id,
    orders.cancellation.source,
    orders.cancellation.reason
  ) AS cancellation,
  STRUCT(
    orders.vendor.id,
    `{project_id}`.pandata_intermediate.LG_UUID(orders.vendor.id, orders.country_code) AS uuid,
    orders.vendor.vendor_code AS code,
    orders.vendor.name,
    orders.vendor.location AS location_geo,
    orders.vendor.vertical_type
  ) AS vendor,
FROM `fulfillment-dwh-production.curated_data_shared.orders` AS orders
LEFT JOIN orders_agg_deliveries
       ON orders.country_code = orders_agg_deliveries.country_code
      AND orders.order_id = orders_agg_deliveries.order_id
LEFT JOIN orders_agg_porygon
       ON orders.country_code = orders_agg_porygon.country_code
      AND orders.order_id = orders_agg_porygon.order_id
