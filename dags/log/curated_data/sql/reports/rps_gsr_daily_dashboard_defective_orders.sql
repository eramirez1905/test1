CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.rps_gsr_daily_dashboard_defective_orders`
PARTITION BY order_date
CLUSTER BY brand_id, entity_id, delivery_type, application_client_name AS
WITH entities AS (
  SELECT en.region_short_name AS region
    , p.entity_id
    , p.brand_name AS delivery_platform
  FROM `{{ params.project_id }}.cl.entities` en
  CROSS JOIN UNNEST(platforms) p
), auto_transition_orders AS (
  SELECT entity_id
    , order_code
    , MAX(is_auto_transition_pickup) AS is_auto_transition_pickup
    , MAX(is_auto_transition_dropoff) AS is_auto_transition_dropoff
  FROM (
    SELECT o.entity.id AS entity_id
      , o.platform_order_code AS order_code
      , d.auto_transition.pickup AS is_auto_transition_pickup
      , d.auto_transition.dropoff AS is_auto_transition_dropoff
    FROM `{{ params.project_id }}.cl._orders` o
    LEFT JOIN `{{ params.project_id }}.cl._deliveries` ds ON o.country_code = ds.country_code
      AND o.order_id = ds.order_id
    LEFT JOIN UNNEST(ds.deliveries) d
    WHERE o.created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 3 MONTH)
      AND d.is_primary
  )
  GROUP BY 1,2
), rps_orders AS (
  SELECT DATE(o.created_at, o.timezone) AS created_date
    , o.region
    , e.delivery_platform
    , o.country_code
    , o.entity.id AS entity_id
    , o.entity.brand_id AS brand_id
    , o.transmission_flow
    , o.service
    , o.is_preorder
    , o.delivery_type
    , COALESCE(o.client.name, 'UNKNOWN') AS application_client_name
    , o.client.version AS application_client_version
    , IF(o.client.name IS NULL, 'UNKNOWN', COALESCE(o.client.pos_integration_flow, '(N/A)')) AS application_integration_flow
    , IF(o.client.name IS NULL, 'UNKNOWN', COALESCE(o.client.wrapper_type, '(N/A)')) AS application_client_wrapper_type
    , o.order_status AS status
    , o.order_id
    , ato.is_auto_transition_pickup
    , ato.is_auto_transition_dropoff
    , IF(ato.is_auto_transition_pickup IS NULL
      , CASE
          WHEN TIMESTAMP_DIFF(o.near_pickup_at, o.estimated_pickup_at, SECOND) <= 0
              AND TIMESTAMP_DIFF(o.picked_up_at, o.estimated_pickup_at, SECOND) <= 0
            THEN 0
          WHEN TIMESTAMP_DIFF(o.near_pickup_at, o.estimated_pickup_at, SECOND) <= 0
              AND TIMESTAMP_DIFF(o.picked_up_at, o.estimated_pickup_at, SECOND) > 0
            THEN TIMESTAMP_DIFF(o.picked_up_at, o.estimated_pickup_at, SECOND)
          WHEN TIMESTAMP_DIFF(o.near_pickup_at, o.estimated_pickup_at, SECOND) BETWEEN 0 AND 600
              AND TIMESTAMP_DIFF(o.picked_up_at, o.near_pickup_at, SECOND) >= 300
            THEN  TIMESTAMP_DIFF(o.picked_up_at, o.estimated_pickup_at, SECOND)
          WHEN TIMESTAMP_DIFF(o.near_pickup_at, o.estimated_pickup_at, SECOND) BETWEEN 0 AND 600
              AND TIMESTAMP_DIFF(o.picked_up_at, o.near_pickup_at, SECOND) < 300
            THEN GREATEST(TIMESTAMP_DIFF(o.picked_up_at, o.near_pickup_at, SECOND) , 0)
          WHEN TIMESTAMP_DIFF(o.near_pickup_at, o.estimated_pickup_at, SECOND) > 600
            THEN NULL
        END
        , NULL
      ) AS vendor_late
    , IF(ato.is_auto_transition_pickup IS NULL
         , TIMESTAMP_DIFF(o.near_pickup_at, o.estimated_pickup_at, SECOND)
         , NULL) AS rider_late
    , IF(ato.is_auto_transition_dropoff IS NULL
         , TIMESTAMP_DIFF(o.near_dropoff_at, COALESCE(o.original_estimated_deliver_at, o.estimated_deliver_at), SECOND)
         , NULL) AS delivery_late
    , IF(order_status = 'cancelled' AND gfr.reason IS NULL, 'PLATFORM', o.cancellation.owner) AS cancellation_owner
    , IF(order_status = 'cancelled' AND gfr.reason IS NULL, 'GFR_INCORRECT', o.cancellation.reason) AS cancellation_reason
    , o.cancellation.source AS cancellation_source
    , o.received_at
    , o.waiting_for_transport_at
    , o.sending_to_vendor_at
    , o.received_by_vendor_at
    , o.modified_by_vendor_at
    , o.reconciliation_started_at
    , o.reconciled_with_platform_at
    , o.reconciled_with_transport_at
    , o.accepted_by_vendor_at
    , o.preparation_minutes
    , o.implicitly_accepted_by_vendor_at
    , o.estimated_pickup_at
    , o.near_pickup_at
    , o.picked_up_at
    , o.promised_for
    , o.estimated_deliver_at
    , o.original_estimated_deliver_at
    , o.delivered_at
    , o.cancelled_at
    , TIMESTAMP_DIFF(o.waiting_for_transport_at, o.received_at, MILLISECOND) AS sent_to_logistics
    , TIMESTAMP_DIFF(o.sending_to_vendor_at, o.waiting_for_transport_at, MILLISECOND) AS courier_assigned
    , TIMESTAMP_DIFF(o.received_by_vendor_at, o.sending_to_vendor_at, MILLISECOND) AS transmission_time
    , TIMESTAMP_DIFF(CASE
                      WHEN COALESCE(o.accepted_by_vendor_at, o.modified_by_vendor_at) IS NOT NULL
                        THEN LEAST(COALESCE(o.accepted_by_vendor_at, o.modified_by_vendor_at), COALESCE(o.modified_by_vendor_at, o.accepted_by_vendor_at))
                      WHEN o.cancelled_at IS NOT NULL
                       AND o.cancellation.source = 'VENDOR_DEVICE'
                       AND o.received_by_vendor_at IS NOT NULL
                       AND o.picked_up_at IS NULL
                       AND o.delivered_at IS NULL
                        THEN o.cancelled_at
                      ELSE NULL
                     END
                     , o.received_by_vendor_at, MILLISECOND) AS response_time
     , IF(o.delivery_type = 'OWN_DELIVERY'
          , TIMESTAMP_DIFF(o.reconciled_with_transport_at, o.modified_by_vendor_at, MILLISECOND)
          , TIMESTAMP_DIFF(o.reconciled_with_platform_at, o.modified_by_vendor_at, MILLISECOND)
         ) AS reconciliation_time
  FROM `{{ params.project_id }}.cl.rps_orders` o
  INNER JOIN entities e ON o.entity.id = e.entity_id
  LEFT JOIN `{{ params.project_id }}.cl._rps_gfr_cancellations` gfr ON o.cancellation.reason = gfr.reason
    AND o.cancellation.owner = gfr.owner
    AND o.delivery_type = gfr.delivery_type
  LEFT JOIN auto_transition_orders ato ON o.entity.id = ato.entity_id
    AND o.order_code = ato.order_code
  WHERE o.created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 3 MONTH)
    AND DATE(o.created_at, o.timezone) < '{{ next_ds }}'
    AND (o.transmission_flow = 'Restaurant' OR o.transmission_flow IS NULL)
), rps_orders_agg AS (
SELECT o.region
  , o.created_date
  , o.delivery_platform
  , o.country_code
  , o.entity_id
  , o.brand_id
  , o.service
  , o.is_preorder
  , o.delivery_type
  , o.status
  , o.application_client_name
  , o.application_integration_flow
  , o.application_client_wrapper_type
  , o.cancellation_owner
  , o.cancellation_reason
  , o.cancellation_source
  , COUNT(DISTINCT o.order_id) AS no_orders
  , COUNT(DISTINCT IF(o.is_auto_transition_pickup IS NULL, o.order_id, NULL)) AS no_orders_without_auto_pickup_transition
  , COUNT(DISTINCT IF(o.is_auto_transition_dropoff IS NULL, o.order_id, NULL)) AS no_orders_without_auto_dropoff_transition
  , COUNT(DISTINCT IF(o.delivery_type = 'OWN_DELIVERY', o.order_id, NULL)) AS no_orders_od
  , COUNT(DISTINCT CASE
                     WHEN o.delivery_type = 'OWN_DELIVERY'
                      AND o.status = 'cancelled'
                      AND o.waiting_for_transport_at IS NULL
                      AND o.sending_to_vendor_at IS NULL
                      AND o.received_by_vendor_at IS NULL
                      AND o.accepted_by_vendor_at IS NULL
                      AND o.modified_by_vendor_at IS NULL
                      AND o.picked_up_at IS NULL
                      AND o.delivered_at IS NULL
                      AND o.cancellation_owner IN ('VENDOR', 'TRANSPORT')
                       THEN o.order_id
                     ELSE NULL
                   END) AS no_failed_orders_od_send_to_logistics
  , COUNT(DISTINCT CASE
                     WHEN o.delivery_type = 'OWN_DELIVERY'
                      AND o.waiting_for_transport_at IS NOT NULL
                       THEN o.order_id
                     ELSE NULL
                   END) AS no_orders_od_received_by_logistics
  , COUNT(DISTINCT CASE
                     WHEN o.delivery_type = 'OWN_DELIVERY'
                      AND o.status = 'cancelled'
                      AND o.waiting_for_transport_at IS NOT NULL
                      AND o.sending_to_vendor_at IS NULL
                      AND o.received_by_vendor_at IS NULL
                      AND o.accepted_by_vendor_at IS NULL
                      AND o.modified_by_vendor_at IS NULL
                      AND o.picked_up_at IS NULL
                      AND o.delivered_at IS NULL
                      AND o.cancellation_owner IN ('VENDOR', 'TRANSPORT')
                       THEN o.order_id
                     ELSE NULL
                   END) AS no_failed_orders_od_send_to_vendor
  , COUNT(DISTINCT CASE
                     WHEN o.delivery_type = 'OWN_DELIVERY'
                      AND o.status = 'cancelled'
                      AND o.sending_to_vendor_at IS NULL
                      AND o.received_by_vendor_at IS NULL
                      AND o.accepted_by_vendor_at IS NULL
                      AND o.modified_by_vendor_at IS NULL
                      AND o.picked_up_at IS NULL
                      AND o.delivered_at IS NULL
                      AND o.cancellation_owner IN ('CUSTOMER', 'PLATFORM')
                       THEN o.order_id
                     ELSE NULL
                   END) AS no_cancelled_orders_od_before_send_to_vendor
  , COUNT(DISTINCT CASE
                     WHEN o.delivery_type = 'OWN_DELIVERY'
                      AND o.sending_to_vendor_at IS NOT NULL
                       THEN o.order_id
                     ELSE NULL
                   END) AS no_orders_od_sent_to_vendor
  , COUNT(DISTINCT CASE
                     WHEN o.delivery_type = 'OWN_DELIVERY'
                      AND o.sending_to_vendor_at IS NOT NULL
                      AND courier_assigned > 8 * 60 * 1000 -- 8 minutes
                       THEN o.order_id
                     ELSE NULL
                   END) AS no_orders_od_sent_to_vendor_above_threshold
  , COUNT(DISTINCT IF(o.sending_to_vendor_at IS NOT NULL, o.order_id, NULL)) AS no_orders_sent_to_vendor
  , COUNT(DISTINCT CASE
                     WHEN o.status = 'cancelled'
                      AND o.sending_to_vendor_at IS NOT NULL
                      AND o.received_by_vendor_at IS NULL
                      AND o.accepted_by_vendor_at IS NULL
                      AND o.modified_by_vendor_at IS NULL
                      AND o.picked_up_at IS NULL
                      AND o.delivered_at IS NULL
                      AND o.cancellation_owner IN ('VENDOR', 'TRANSPORT')
                       THEN o.order_id
                     ELSE NULL
                    END) AS no_failed_orders_transmit_to_vendor
  , COUNT(DISTINCT IF(o.received_by_vendor_at IS NOT NULL, o.order_id, NULL)) AS no_orders_received_by_vendor
  , COUNT(DISTINCT CASE
                     WHEN o.received_by_vendor_at IS NOT NULL
                      AND transmission_time > 5 * 1000 -- 5 seconds
                       THEN o.order_id
                     ELSE NULL
                    END) AS no_orders_received_by_vendor_above_threshold
  , COUNT(DISTINCT CASE
                     WHEN o.status = 'cancelled'
                      AND o.received_by_vendor_at IS NOT NULL
                      AND o.accepted_by_vendor_at IS NULL
                      AND o.modified_by_vendor_at IS NULL
                      AND o.picked_up_at IS NULL
                      AND o.delivered_at IS NULL
                      AND (o.cancellation_source != 'VENDOR_DEVICE' OR o.cancellation_source IS NULL) -- if cancellation_source = 'VENDOR_DEVICE' this means that the vendor responded
                      AND o.cancellation_owner IN ('VENDOR', 'TRANSPORT')
                       THEN o.order_id
                     ELSE NULL
                   END) AS no_failed_orders_without_vendor_response
  , COUNT(DISTINCT CASE
                     WHEN o.status = 'cancelled'
                      AND o.sending_to_vendor_at IS NOT NULL
                      AND o.accepted_by_vendor_at IS NULL
                      AND o.modified_by_vendor_at IS NULL
                      AND o.picked_up_at IS NULL
                      AND o.delivered_at IS NULL
                      AND (o.cancellation_source != 'VENDOR_DEVICE' OR o.cancellation_source IS NULL) -- if cancellation_source = 'VENDOR_DEVICE' this means that the vendor responded
                      AND o.cancellation_owner IN ('CUSTOMER', 'PLATFORM')
                       THEN o.order_id
                     ELSE NULL
                   END) AS no_cancelled_orders_before_vendor_response
  , COUNT(DISTINCT CASE
                     WHEN o.status = 'cancelled'
                      AND o.received_by_vendor_at IS NOT NULL
                      AND o.accepted_by_vendor_at IS NULL
                      AND o.modified_by_vendor_at IS NULL
                      AND o.picked_up_at IS NULL
                      AND o.delivered_at IS NULL
                      AND o.cancellation_source = 'VENDOR_DEVICE'
                       THEN o.order_id
                     ELSE NULL
                   END) AS no_orders_declined_by_vendor
  , COUNT(DISTINCT CASE
                     WHEN o.received_by_vendor_at IS NOT NULL
                      AND o.modified_by_vendor_at IS NOT NULL
                       THEN o.order_id
                     ELSE NULL
                   END) AS no_orders_modified_by_vendor
  , COUNT(DISTINCT CASE
                     WHEN o.received_by_vendor_at IS NOT NULL
                      AND o.modified_by_vendor_at IS NOT NULL
                      AND ((o.delivery_type = 'OWN_DELIVERY' AND o.reconciled_with_transport_at IS NOT NULL) OR
                           (o.delivery_type != 'OWN_DELIVERY' AND o.reconciled_with_platform_at IS NOT NULL)
                          )
                       THEN o.order_id
                     ELSE NULL
                   END) AS no_orders_reconciled
  , COUNT(DISTINCT CASE
                     WHEN o.received_by_vendor_at IS NOT NULL
                      AND o.modified_by_vendor_at IS NOT NULL
                      AND ((o.delivery_type = 'OWN_DELIVERY' AND o.reconciled_with_transport_at IS NOT NULL) OR
                           (o.delivery_type != 'OWN_DELIVERY' AND o.reconciled_with_platform_at IS NOT NULL)
                          )
                      AND o.status = 'completed'
                       THEN o.order_id
                     ELSE NULL
                   END) AS no_orders_reconciled_and_completed
  , COUNT(DISTINCT CASE
                     WHEN (o.received_by_vendor_at IS NOT NULL)
                      AND ((o.modified_by_vendor_at IS NOT NULL) OR
                           (o.accepted_by_vendor_at IS NOT NULL) OR
                           (o.cancelled_at IS NOT NULL AND
                            o.cancellation_source = 'VENDOR_DEVICE' AND
                            o.picked_up_at IS NULL AND
                            o.delivered_at IS NULL)
                          )
                       THEN o.order_id
                     ELSE NULL
                   END) AS no_orders_with_vendor_response -- vendor received the order and either accepted, modified or rejected it using the device
  , COUNT(DISTINCT CASE
                     WHEN (o.received_by_vendor_at IS NOT NULL)
                      AND ((o.modified_by_vendor_at IS NOT NULL) OR
                           (o.accepted_by_vendor_at IS NOT NULL) OR
                           (o.cancelled_at IS NOT NULL AND
                            o.cancellation_source = 'VENDOR_DEVICE' AND
                            o.picked_up_at IS NULL AND
                            o.delivered_at IS NULL)
                          )
                      AND (response_time > 30 * 1000) -- 30 seconds
                       THEN o.order_id
                     ELSE NULL
                   END) AS no_orders_with_vendor_response_above_threshold
  , COUNT(DISTINCT IF(o.implicitly_accepted_by_vendor_at IS NOT NULL, o.order_id, NULL)) AS no_orders_implicitly_accepted_by_vendor
  , COUNT(DISTINCT CASE
                     WHEN o.delivery_type = 'OWN_DELIVERY'
                      AND o.picked_up_at IS NOT NULL
                       THEN o.order_id
                     ELSE NULL
                   END) AS no_orders_picked_up
  , COUNT(DISTINCT CASE
                     WHEN o.delivery_type = 'OWN_DELIVERY'
                      AND o.picked_up_at IS NOT NULL
                      AND TIMESTAMP_DIFF(o.picked_up_at, o.estimated_pickup_at, SECOND) > 600
                       THEN o.order_id
                     ELSE NULL
                   END) AS no_orders_picked_up_late_10
  , COUNT(DISTINCT CASE
                    WHEN o.delivery_type = 'OWN_DELIVERY'
                     AND o.picked_up_at IS NOT NULL
                     AND o.vendor_late IS NOT NULL
                      THEN o.order_id
                    ELSE NULL
                   END) AS no_orders_picked_up_without_auto_pickup_transition_rider_10
  , COUNT(DISTINCT CASE
                     WHEN o.delivery_type = 'OWN_DELIVERY'
                      AND o.picked_up_at IS NOT NULL
                      AND o.vendor_late > 600
                       THEN o.order_id
                     ELSE NULL
                   END) AS no_orders_od_picked_up_late_vendor_10
  , COUNT(DISTINCT CASE
                     WHEN o.delivery_type = 'OWN_DELIVERY'
                      AND o.delivered_at IS NOT NULL
                      AND o.is_auto_transition_dropoff IS NULL
                       THEN o.order_id
                     ELSE NULL
                   END) AS no_orders_od_delivered_without_auto_dropoff_transition
  , COUNT(DISTINCT CASE
                     WHEN o.delivery_type = 'OWN_DELIVERY'
                      AND o.delivered_at IS NOT NULL
                      AND o.is_auto_transition_dropoff IS NULL
                      AND o.delivery_late > 600
                       THEN o.order_id
                     ELSE NULL
                   END) AS no_orders_od_delivered_late_10
  , COUNT(DISTINCT CASE
                     WHEN o.delivery_type = 'OWN_DELIVERY'
                      AND o.delivered_at IS NOT NULL
                      AND o.is_auto_transition_dropoff IS NULL
                      AND o.delivery_late > 600
                      AND o.picked_up_at <= o.estimated_pickup_at
                       THEN o.order_id
                     ELSE NULL
                   END) AS no_orders_od_delivered_late_10_picked_up_on_time
  , COUNT(DISTINCT CASE
                     WHEN o.delivery_type = 'VENDOR_DELIVERY'
                      AND o.status = 'completed'
                       THEN o.order_id
                     ELSE NULL
                   END) AS no_orders_vd_completed
  , COUNT(DISTINCT CASE
                     WHEN o.delivery_type = 'VENDOR_DELIVERY'
                      AND o.status = 'completed'
                      AND o.picked_up_at IS NULL
                       THEN o.order_id
                     ELSE NULL
                   END) AS no_orders_vd_completed_not_dispatched
  , COUNT(DISTINCT CASE
                     WHEN o.delivery_type = 'VENDOR_DELIVERY'
                      AND o.status = 'completed'
                      AND o.picked_up_at IS NOT NULL
                       THEN o.order_id
                     ELSE NULL
                   END) AS no_orders_vd_completed_dispatched
  , COUNT(DISTINCT CASE
                     WHEN o.delivery_type = 'VENDOR_DELIVERY'
                      AND o.status = 'completed'
                      AND o.picked_up_at IS NOT NULL
                      AND TIMESTAMP_DIFF(COALESCE(o.original_estimated_deliver_at, o.estimated_deliver_at), o.picked_up_at, SECOND) < 300
                       THEN o.order_id
                     ELSE NULL
                   END) AS no_orders_vd_completed_dispatched_late -- orders dispatched after or less than 5min before estimated delivery time, means that either vendor hit the dispatch button late or the order was probably delivered late
  , COUNT(DISTINCT CASE
                     WHEN o.delivery_type = 'PICKUP'
                      AND o.status = 'completed'
                       THEN o.order_id
                     ELSE NULL
                   END) AS no_orders_pu_completed
  , COUNT(DISTINCT CASE
                     WHEN o.delivery_type = 'PICKUP'
                      AND o.status = 'completed'
                      AND o.delivered_at IS NULL
                       THEN o.order_id
                     ELSE NULL
                   END) AS no_orders_pu_completed_not_confirmed
   , COUNT(DISTINCT CASE
                      WHEN o.delivery_type = 'OWN_DELIVERY'
                       AND o.preparation_minutes > 5
                        THEN o.order_id
                      ELSE NULL
                    END) AS no_orders_od_preptime_above_a
   , COUNT(DISTINCT CASE
                      WHEN o.delivery_type = 'OWN_DELIVERY'
                       AND o.preparation_minutes > 10
                        THEN o.order_id
                      ELSE NULL
                    END) AS no_orders_od_preptime_above_b
   , COUNT(DISTINCT CASE
                      WHEN o.delivery_type = 'OWN_DELIVERY'
                       AND o.preparation_minutes > 20
                        THEN o.order_id
                      ELSE NULL
                    END) AS no_orders_od_preptime_above_c
   , COUNT(DISTINCT CASE
                      WHEN o.delivery_type = 'OWN_DELIVERY'
                       AND TIMESTAMP_DIFF(COALESCE(o.original_estimated_deliver_at, o.estimated_deliver_at), o.sending_to_vendor_at, MINUTE) > 45
                        THEN o.order_id
                      ELSE NULL
                    END) AS no_orders_od_long_estimated_delivery_time
   , COUNT(DISTINCT IF(o.delivery_type = 'VENDOR_DELIVERY', o.order_id, NULL)) AS no_orders_vd
   , COUNT(DISTINCT CASE
                      WHEN o.delivery_type = 'VENDOR_DELIVERY'
                       AND TIMESTAMP_DIFF(COALESCE(o.original_estimated_deliver_at, o.estimated_deliver_at), o.sending_to_vendor_at, MINUTE) > 32
                        THEN o.order_id
                      ELSE NULL
                    END) AS no_orders_vd_eta_above_a
   , COUNT(DISTINCT CASE
                      WHEN o.delivery_type = 'VENDOR_DELIVERY'
                       AND TIMESTAMP_DIFF(COALESCE(o.original_estimated_deliver_at, o.estimated_deliver_at), o.sending_to_vendor_at, MINUTE) > 47
                        THEN o.order_id
                      ELSE NULL
                    END) AS no_orders_vd_eta_above_b
   , COUNT(DISTINCT CASE
                      WHEN o.delivery_type = 'VENDOR_DELIVERY'
                       AND TIMESTAMP_DIFF(COALESCE(o.original_estimated_deliver_at, o.estimated_deliver_at), o.sending_to_vendor_at, MINUTE) > 62
                        THEN o.order_id
                      ELSE NULL
                    END) AS no_orders_vd_eta_above_c
   , COUNT(DISTINCT CASE
                      WHEN o.delivery_type = 'VENDOR_DELIVERY'
                       AND o.application_client_name = 'GODROID'
                       AND o.application_client_version >= '4.5.0'
                       AND o.promised_for IS NOT NULL -- make sure we have a suggestion for the vendor
                       AND o.estimated_deliver_at IS NOT NULL -- make sure we have the time vendor selected
                        THEN o.order_id
                      ELSE NULL
                    END) AS no_orders_vd_valid_godroid_version
   , COUNT(DISTINCT CASE
                      WHEN o.delivery_type = 'VENDOR_DELIVERY'
                       AND o.application_client_name = 'GODROID'
                       AND o.application_client_version >= '4.5.0'
                       AND TIMESTAMP_DIFF(COALESCE(o.original_estimated_deliver_at, o.estimated_deliver_at), o.promised_for, MINUTE) >= -5
                       AND TIMESTAMP_DIFF(COALESCE(o.original_estimated_deliver_at, o.estimated_deliver_at), o.promised_for, MINUTE) <= 5
                        THEN o.order_id
                      ELSE NULL
                    END) AS no_orders_vd_valid_godroid_version_on_time
   , COUNT(DISTINCT CASE
                      WHEN o.delivery_type = 'VENDOR_DELIVERY'
                       AND o.application_client_name = 'GODROID'
                       AND o.application_client_version >= '4.5.0'
                       AND TIMESTAMP_DIFF(COALESCE(o.original_estimated_deliver_at, o.estimated_deliver_at), o.promised_for, MINUTE) < -5
                        THEN o.order_id
                      ELSE NULL
                    END) AS no_orders_vd_valid_godroid_version_before
   , COUNT(DISTINCT CASE
                      WHEN o.delivery_type = 'VENDOR_DELIVERY'
                       AND o.application_client_name = 'GODROID'
                       AND o.application_client_version >= '4.5.0'
                       AND TIMESTAMP_DIFF(COALESCE(o.original_estimated_deliver_at, o.estimated_deliver_at), o.promised_for, MINUTE) > 5
                        THEN o.order_id
                      ELSE NULL
                    END) AS no_orders_vd_valid_godroid_version_after
 FROM rps_orders o
 GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
), all_combinations_base AS (
  WITH order_dates AS (
    SELECT DISTINCT created_date
    FROM rps_orders_agg
  ), regions AS (
    SELECT DISTINCT region
      , delivery_platform
      , country_code
      , entity_id
      , brand_id
    FROM rps_orders_agg
  ), services AS (
    SELECT DISTINCT service
    FROM rps_orders_agg
  ), preorder AS (
    SELECT DISTINCT is_preorder
    FROM rps_orders_agg
  ), delivery_types AS (
    SELECT DISTINCT delivery_type
    FROM rps_orders_agg
  ), order_status AS (
    SELECT DISTINCT status
    FROM rps_orders
  ), clients AS (
    SELECT DISTINCT application_client_name
      , application_integration_flow
      , application_client_wrapper_type
    FROM rps_orders_agg
  ), cancellations AS (
    SELECT DISTINCT cancellation_owner
      , cancellation_reason
      , cancellation_source
    FROM rps_orders_agg
  )
  SELECT *
  FROM order_dates
  CROSS JOIN regions
  CROSS JOIN services
  CROSS JOIN preorder
  CROSS JOIN delivery_types
  CROSS JOIN order_status
  CROSS JOIN clients
  CROSS JOIN cancellations
  WHERE (order_status.status = 'completed' AND cancellations.cancellation_owner IS NULL)
     OR (order_status.status = 'cancelled' AND cancellations.cancellation_owner IS NOT NULL)
  ORDER BY 1, 2, 3, 4, 5, 6
)
SELECT al.region
  , al.created_date AS order_date
  , EXTRACT(YEAR FROM al.created_date) AS order_year
  , FORMAT_DATE('%Y-%m', al.created_date) AS order_month
  , FORMAT_DATE('%Y-%V', al.created_date) AS order_week
  , al.delivery_platform
  , al.country_code
  , al.entity_id
  , al.brand_id
  , al.service
  , al.is_preorder
  , al.delivery_type
  , al.status
  , al.application_client_name
  , al.application_integration_flow
  , al.application_client_wrapper_type
  , al.cancellation_owner
  , al.cancellation_reason
  , al.cancellation_source
  , o.no_orders
  , o.no_orders_without_auto_pickup_transition
  , o.no_orders_without_auto_dropoff_transition
  , o.no_orders_od
  , o.no_failed_orders_od_send_to_logistics
  , o.no_orders_od_received_by_logistics
  , o.no_failed_orders_od_send_to_vendor
  , o.no_cancelled_orders_od_before_send_to_vendor
  , o.no_orders_od_sent_to_vendor
  , o.no_orders_od_sent_to_vendor_above_threshold
  , o.no_orders_sent_to_vendor
  , o.no_failed_orders_transmit_to_vendor
  , o.no_orders_received_by_vendor
  , o.no_orders_received_by_vendor_above_threshold
  , o.no_failed_orders_without_vendor_response
  , o.no_cancelled_orders_before_vendor_response
  , o.no_orders_declined_by_vendor
  , o.no_orders_modified_by_vendor
  , o.no_orders_reconciled
  , o.no_orders_reconciled_and_completed
  , o.no_orders_with_vendor_response
  , o.no_orders_with_vendor_response_above_threshold
  , o.no_orders_implicitly_accepted_by_vendor
  , o.no_orders_picked_up
  , o.no_orders_picked_up_late_10
  , o.no_orders_picked_up_without_auto_pickup_transition_rider_10
  , o.no_orders_od_picked_up_late_vendor_10
  , o.no_orders_od_delivered_without_auto_dropoff_transition
  , o.no_orders_od_delivered_late_10
  , o.no_orders_od_delivered_late_10_picked_up_on_time
  , o.no_orders_vd_completed
  , o.no_orders_vd_completed_not_dispatched
  , o.no_orders_vd_completed_dispatched
  , o.no_orders_vd_completed_dispatched_late
  , o.no_orders_pu_completed
  , o.no_orders_pu_completed_not_confirmed
  , o.no_orders_od_preptime_above_a
  , o.no_orders_od_preptime_above_b
  , o.no_orders_od_preptime_above_c
  , o.no_orders_od_long_estimated_delivery_time
  , o.no_orders_vd
  , o.no_orders_vd_eta_above_a
  , o.no_orders_vd_eta_above_b
  , o.no_orders_vd_eta_above_c
  , o.no_orders_vd_valid_godroid_version
  , o.no_orders_vd_valid_godroid_version_on_time
  , o.no_orders_vd_valid_godroid_version_before
  , o.no_orders_vd_valid_godroid_version_after
FROM all_combinations_base al
LEFT JOIN rps_orders_agg o ON al.created_date = o.created_date
  AND al.country_code = o.country_code
  AND al.entity_id = o.entity_id
  AND al.service = o.service
  AND al.is_preorder = o.is_preorder
  AND al.delivery_type = o.delivery_type
  AND al.status = o.status
  AND al.application_client_name = o.application_client_name
  AND al.application_integration_flow = o.application_integration_flow
  AND al.application_client_wrapper_type = o.application_client_wrapper_type
  AND COALESCE(al.cancellation_owner, "NONE") = COALESCE(o.cancellation_owner, "NONE")
  AND COALESCE(al.cancellation_reason, "NONE") = COALESCE(o.cancellation_reason, "NONE")
  AND COALESCE(al.cancellation_source, "NONE") = COALESCE(o.cancellation_source, "NONE")
