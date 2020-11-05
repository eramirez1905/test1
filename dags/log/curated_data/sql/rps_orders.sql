CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.rps_orders`
PARTITION BY created_date
CLUSTER BY region, order_id AS
WITH oma_orders AS (
  SELECT *
  FROM `{{ params.project_id }}.cl._rps_oma_orders`
), icash_orders AS (
  SELECT *
    , 'iCash' AS service
    , CAST(NULL AS STRING) AS transmission_flow
  FROM `{{ params.project_id }}.cl._rps_icash_orders`
)
SELECT region
  , service
  , transmission_flow
  , country_code
  , country_iso
  , _rps_global_key
  , entity
  , created_date
  , created_at
  , order_id
  , order_code
  , pos_order_id
  , vendor
  , _operator_code
  , timezone
  , delivery_type
  , is_preorder
  , order_status
  , transitions
  , received_at
  , waiting_for_transport_at
  , sending_to_vendor_at
  , received_by_vendor_at
  , modified_by_vendor_at
  , reconciliation_started_at
  , reconciled_with_platform_at
  , reconciled_with_transport_at
  , CAST(minutes_to_accept AS INT64) AS minutes_to_accept
  , accepted_by_vendor_at
  , implicitly_accepted_by_vendor_at
  , prepared_at
  , CAST(preparation_minutes AS INT64) AS preparation_minutes
  , estimated_pickup_at
  , near_pickup_at
  , picked_up_at
  , near_dropoff_at
  , promised_for
  , estimated_deliver_at
  , original_estimated_deliver_at
  , delivered_at
  , cancelled_at
  , CAST(NULL AS STRING) AS _delivery_state
  , stage
  , STRUCT(
      TIMESTAMP_DIFF(received_by_vendor_at, sending_to_vendor_at, SECOND) AS transmission_time
      , TIMESTAMP_DIFF(accepted_by_vendor_at, received_by_vendor_at, SECOND) AS response_time
      , TIMESTAMP_DIFF(picked_up_at, accepted_by_vendor_at, SECOND) AS preparation_time
    ) AS timings
  , IF(order_status = 'cancelled', cancellation, NULL) AS cancellation
  , products
  -- Talabat is directly integrated with OPA/OMA for Grocery, so they are using their own client instead
  , IF(_rps_global_key = 'TB' AND transmission_flow = 'Grocery', NULL, client) AS client
  , device
  , comment
FROM oma_orders

UNION ALL

SELECT o.region
  , o.service
  , o.transmission_flow
  , o.country_code
  , UPPER(o.country_code) AS country_iso
  , o._rps_global_key
  , o.entity
  , CAST(o.created_at AS DATE) AS created_date
  , o.created_at
  , o.order_id
  , o.order_code
  , o.pos_order_id
  , o.vendor
  , o._operator_code
  , o.timezone
  , CASE
      WHEN o.is_own_delivery
        THEN 'OWN_DELIVERY'
      WHEN o._dispatch_state = 'PICKUP'
        THEN 'PICKUP'
      ELSE 'VENDOR_DELIVERY'
    END AS delivery_type
  , o.is_preorder
  , IF(CONCAT('DELIVERY_', o._delivery_state) IN ('DELIVERY_REJECTED',
      'DELIVERY_EXPIRED',
      'DELIVERY_CANCELLED',
      'DELIVERY_MISSED',
      'DELIVERY_CANCELLED_BY_PLATFORM',
      'DELIVERY_CANCELLED_BY_TRANSPORT'
    ), 'cancelled', 'completed') AS order_status
  , o.transitions
  , o.received_at
  , o.waiting_for_transport_at
  , o.sending_to_vendor_at
  -- Fallback due to errors in iCash order flow data not sending the data
  , IF(o.received_by_vendor_at IS NULL AND o.accepted_by_vendor_at IS NOT NULL, o._delivered_to_pos_timestamp,  o.received_by_vendor_at) AS received_by_vendor_at
  , CAST(NULL AS TIMESTAMP) AS modified_by_vendor_at
  , CAST(NULL AS TIMESTAMP) AS reconciliation_started_at
  , CAST(NULL AS TIMESTAMP) AS reconciled_with_platform_at
  , CAST(NULL AS TIMESTAMP) AS reconciled_with_transport_at
  , CAST(NULL AS INT64) AS minutes_to_accept
  , o.accepted_by_vendor_at
  , CAST(NULL AS TIMESTAMP) AS implicitly_accepted_by_vendor_at
  , CAST(NULL AS TIMESTAMP) AS prepared_at
  , IF(o.is_own_delivery, TIMESTAMP_DIFF(o._transport_pickup_time, o._delivery_timestamp, MINUTE), NULL) AS preparation_minutes
  , o._transport_pickup_time AS estimated_pickup_at
  , o.near_pickup_at
  , o.picked_up_at
  , o.near_dropoff_at
  , o.promised_for
  , o.estimated_deliver_at
  , o.original_estimated_deliver_at
  , o.delivered_at
  -- LOGDEP-1230 would help fixing the NULL values here
  , IF(CONCAT('DELIVERY_', _delivery_state) IN ('DELIVERY_REJECTED'
      , 'DELIVERY_EXPIRED'
      , 'DELIVERY_CANCELLED'
      , 'DELIVERY_MISSED'
      , 'DELIVERY_CANCELLED_BY_PLATFORM'
      , 'DELIVERY_CANCELLED_BY_TRANSPORT'
    ), o.cancelled_at, NULL) AS cancelled_at
  , o._delivery_state
  , (SELECT stage FROM os.order_flow_stages fs WHERE o.last_transition IN UNNEST(states)) AS stage
  , STRUCT (
        TIMESTAMP_DIFF(o.received_by_vendor_at, o.sending_to_vendor_at, SECOND) AS transmission_time
      , TIMESTAMP_DIFF(o.accepted_by_vendor_at, o.received_by_vendor_at, SECOND) AS response_time
      , TIMESTAMP_DIFF(o.picked_up_at, o.accepted_by_vendor_at, SECOND) AS preparation_time
    ) AS timings
  , IF(
      CONCAT('DELIVERY_', _delivery_state) IN ('DELIVERY_REJECTED'
          , 'DELIVERY_EXPIRED'
          , 'DELIVERY_CANCELLED'
          , 'DELIVERY_MISSED'
          , 'DELIVERY_CANCELLED_BY_PLATFORM'
          , 'DELIVERY_CANCELLED_BY_TRANSPORT')
      , CASE
          WHEN oc.cancellation IS NOT NULL
            THEN oc.cancellation
          WHEN o._delivery_state IN ('EXPIRED', 'MISSED')
            THEN (SELECT AS STRUCT reason, owner, is_cancelled_by_vendor, CAST(NULL AS STRING) AS stage, CAST(NULL AS STRING) AS source
                FROM UNNEST(os.oma_fail_reasons) fr WHERE o._delivery_state = fr.state)
          ELSE (SELECT AS STRUCT reason, owner, is_cancelled_by_vendor, CAST(NULL AS STRING) AS stage, CAST(NULL AS STRING) AS source
              FROM UNNEST(os.fail_reasons) fr WHERE o._reject_reason = fr.id)
        END
      , NULL
    ) AS cancellation
  , NULL AS products
  , o.client
  , o.device
  , o.comment
FROM icash_orders o
LEFT JOIN `{{ params.project_id }}.cl._rps_icash_order_cancellation` oc ON o.order_id = oc.order_id
  AND o.region = oc.region
CROSS JOIN `{{ params.project_id }}.cl._rps_order_status` AS os
WHERE (oc.cancellation.reason != 'TEST_ORDER' OR oc.cancellation.reason IS NULL)
