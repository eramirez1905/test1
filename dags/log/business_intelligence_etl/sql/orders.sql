CREATE OR REPLACE TABLE il.orders
PARTITION BY created_date
CLUSTER BY country_code, order_id AS
WITH delivery_time_dataset AS (
    SELECT d.country_code,
      o.id AS order_id,
      d.status,
      d.redelivery,
      first_value(
        CASE WHEN redelivery IS FALSE
          THEN dt.vendor_late
        END
      ) OVER (PARTITION BY o.country_code, o.id ORDER BY dt.created_at) / 60 AS vendor_late,
      first_value(
        CASE WHEN redelivery IS FALSE
          THEN dt.courier_late
        END
      ) OVER (PARTITION BY o.country_code, o.id ORDER BY dt.created_at) / 60 AS courier_late,
      d.courier_picked_up_at,
      d.courier_dropped_off_at,
      to_dropoff_time
    FROM `{{ params.project_id }}.ml.hurrier_orders` o
    LEFT JOIN `{{ params.project_id }}.ml.hurrier_deliveries` d ON d.country_code = o.country_code
      AND o.id = d.order_id
    LEFT JOIN `{{ params.project_id }}.ml.hurrier_delivery_timings` dt ON d.country_code = dt.country_code
      AND d.id = dt.delivery_id
    WHERE d.status = 'completed'
), orders_transitions AS (
  SELECT ot.country_code
    , ot.order_id AS order_id
    , ot.id AS order_transition_id
    , ot.to_state
    , ot.most_recent
    , ot.created_at
    , CAST(JSON_EXTRACT_SCALAR(ot.metadata, '$.user_id') AS INT64) AS user_id
    , CAST((JSON_EXTRACT_SCALAR(ot.metadata, '$.performed_by') = 'unn') AS BOOL) AS system_cancelled
    , CAST((JSON_EXTRACT_SCALAR(ot.metadata, '$.performed_by') = 'issue_service') AS BOOL) AS issue_service_cancelled
    , CAST((JSON_EXTRACT_SCALAR(ot.metadata, '$.performed_by') = 'dispatcher') AS BOOL) AS dispatcher_cancelled
    , CASE
        WHEN JSON_EXTRACT_SCALAR(metadata, '$.performed_by') = 'dispatcher'
          THEN CAST(JSON_EXTRACT_SCALAR(metadata, '$.reason') AS STRING)
        END AS cancellation_reason
  FROM `{{ params.project_id }}.ml.hurrier_order_transitions` ot
  WHERE JSON_EXTRACT_SCALAR(ot.metadata, '$.performed_by') IS NOT NULL
), last_rider_at_customer_dataset AS (
  SELECT country_code,
    order_id,
    status,
    MAX(TIMESTAMP_ADD(courier_picked_up_at, INTERVAL to_dropoff_time second)) AS last_rider_at_customer
  FROM delivery_time_dataset
  WHERE redelivery IS FALSE
  GROUP BY 1, 2, 3
), delivery_timings AS (
  SELECT country_code,
    order_id,
    status,
    MAX(vendor_late) AS vendor_late,
    MAX(courier_late) AS courier_late,
    MIN(courier_picked_up_at) AS first_rider_pick_up,
    MAX(courier_dropped_off_at) AS last_food_delivered,
    MAX(TIMESTAMP_ADD(courier_picked_up_at, INTERVAL to_dropoff_time second)) AS last_rider_at_customer_incl_redelivery
  FROM delivery_time_dataset
  GROUP BY 1, 2, 3
), datase_tags AS (
  SELECT tags
  FROM `{{ params.project_id }}.ml.hurrier_orders` o
), hu_fct_orders_temp AS (
  SELECT o.country_code
    , o.id AS order_id
    , CASE
      WHEN o.platform IN ('MJAM', 'ONLINE_PIZZA_SE')
        THEN NULL
      ELSE o.unn_order_id
      END AS legacy_order_id
    , o.confirmation_number AS order_code
    , 0 AS dispatcher_internal_id
    , 0 AS delivery_provider_internal_id
    , o.featured_business_id AS business_id
    , FALSE AS rider_stacked
    , (1.0 * o.total / 100) AS order_amount
    , "" AS paymenttype_name
    , o.original_scheduled_pickup_at AS dispatcher_expected_pick_up_time
    , o.scheduled_dropoff_at AS dispatcher_expected_delivery_time
    , o.created_at AS disp_created_at
    , o.order_placed_at AS created_at
    , o.order_placed_at AS time_order_started
    , dt.first_rider_pick_up AS food_picked_up
    , COALESCE(lrc.last_rider_at_customer, dt.last_rider_at_customer_incl_redelivery) AS rider_at_customer
    , dt.last_food_delivered AS food_delivered
    , DATETIME(dt.last_food_delivered, ci.timezone) AS food_delivered_local
    , ci.timezone
    , CAST(dt.last_food_delivered AS DATE) AS report_date
    , CAST(DATETIME(dt.last_food_delivered, ci.timezone) AS DATE) AS report_date_local
    , (o.estimated_prep_buffer / 60) AS estimated_prep_buffer
    , (o.estimated_prep_duration / 60) AS estimated_prep_duration
    , o.utilization
    , o.pickup_address_id
    , o.dropoff_address_id
    , o.STATUS AS order_status
    , ci.id AS city_id
    , ci.name AS city_name
    , cast('preorder' IN UNNEST(o.tags) as BOOL) AS preorder
    , ad.zip AS dropoff_zipcode
    , o.platform
    , cast('corporate' IN UNNEST(o.tags) as BOOL) AS is_corporate
    , dt.vendor_late
    , dt.courier_late
    , o.tags
    , o.scheduled_pickup_at AS dispatcher_expected_pick_up_time_updated
    , round(((COALESCE(o.total, 0) / 100) - (COALESCE(o.delivery_fee, 0) / 100) / 100) / 1 ,2)  as food_value
    , round(((COALESCE(o.total, 0) / 100) - (COALESCE(o.delivery_fee, 0) / 100) / 100), 2) AS food_local_value
    , round((COALESCE(o.delivery_fee, 0) / 100) / 1, 2) AS delivery_fee
    , round((COALESCE(o.delivery_fee, 0) / 100), 2) AS delivery_fee_local_value
    , 1 as exchange_rate_value
    , NULL AS tag
    , 'new_address' AS address_tag
    , o.order_sent_at
    , o.order_received_at AS order_received_at
    , (o.online_tip/100) AS rider_tip_local
    , o.cod_pay_at_pickup
    , o.cod_collect_at_dropoff
    , o.updated_at
    , b.vendor_code
    , b.vendor_name
    , b.walk_in_time AS vendor_walk_in_time
    , b.walk_out_time AS vendor_walk_out_time
    , array((
        SELECT AS STRUCT * EXCEPT (country_code, order_id)
        FROM orders_transitions ot
        WHERE ot.country_code = o.country_code
          AND ot.order_id = o.id
    )) AS transitions
  FROM `{{ params.project_id }}.ml.hurrier_orders` o
  LEFT JOIN `{{ params.project_id }}.ml.hurrier_addresses` ad ON ad.country_code = o.country_code
    AND ad.id = COALESCE(o.dropoff_address_id, o.pickup_address_id)
  LEFT JOIN `{{ params.project_id }}.ml.hurrier_cities` ci ON ci.country_code = o.country_code
    AND ad.city_id = ci.id
  LEFT JOIN il.businesses b ON o.featured_business_id = b.business_id
    AND o.country_code = b.country_code
  LEFT JOIN delivery_timings dt ON dt.order_id = o.id
    AND o.country_code = dt.country_code
  LEFT JOIN last_rider_at_customer_dataset lrc ON lrc.order_id = o.id
    AND o.country_code = lrc.country_code
)
SELECT o.country_code
    , o.order_id
    , o.legacy_order_id
    , o.order_code
    , o.rider_stacked
    , o.order_amount
    , o.created_at
    , cast(o.created_at AS DATE) AS created_date
    , o.food_picked_up
    , o.rider_at_customer
    , o.food_delivered
    , o.food_delivered_local
    , o.timezone
    , o.report_date
    , o.report_date_local
    , o.estimated_prep_buffer
    , o.estimated_prep_duration
    , o.utilization
    , o.disp_created_at
    , o.dispatcher_expected_pick_up_time
    , o.dispatcher_expected_delivery_time
    , TIMESTAMP_DIFF(o.food_delivered, o.dispatcher_expected_delivery_time, SECOND) / 60 AS delivery_delay
    , o.order_status
    , CASE
      WHEN TIMESTAMP_DIFF(o.rider_at_customer, o.created_at, MINUTE) < 180
        THEN TIMESTAMP_DIFF(o.rider_at_customer, o.created_at, SECOND) / 60
      END AS delivery_time_eff
    , TIMESTAMP_DIFF(o.rider_at_customer, o.dispatcher_expected_delivery_time, SECOND) / 60 AS delivery_delay_eff
    , city_id
    , city_name
    , vendor_code
    , vendor_name
    , vendor_walk_in_time
    , vendor_walk_out_time
    , preorder
    , dropoff_zipcode
    , business_id
    , o.platform
    , o.is_corporate
    , o.vendor_late
    , o.courier_late
    , o.tags
    , o.dispatcher_expected_pick_up_time_updated
    , o.delivery_fee
    , o.food_value
    , o.food_local_value
    , o.delivery_fee_local_value
    , o.exchange_rate_value
    , NULL AS address_tag
    , o.order_sent_at
    , o.order_received_at
    , o.rider_tip_local
    , o.cod_collect_at_dropoff
    , o.cod_pay_at_pickup
    , o.transitions
FROM hu_fct_orders_temp o
;
