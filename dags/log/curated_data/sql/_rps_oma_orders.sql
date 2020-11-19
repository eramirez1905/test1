CREATE TEMP FUNCTION json2array(json STRING)
RETURNS ARRAY<STRING>
LANGUAGE js AS """
  return JSON.parse(json).map(x=>JSON.stringify(x));
""";

CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._rps_oma_orders`
PARTITION BY created_date
CLUSTER BY region, order_id AS
WITH entities AS (
  SELECT region_short_name AS region
    , country_iso
    , LOWER(country_iso) AS country_code
    , entity_id
    , timezone
    , display_name AS entity_display_name
    , brand_id
  FROM `{{ params.project_id }}.cl.entities` c
  LEFT JOIN UNNEST(c.platforms) platform
), vendors AS (
  SELECT v.entity_id
    , v.vendor_code
    , rps.region
    , rps.client.name AS client_name
    , rps.client.pos_integration_flow
    , rps.timezone
    , rps.vendor_id
    , rps.operator_code AS _operator_code
    , rps.contracts.delivery_type
    , rps.rps_global_key
    , rps.is_active
  FROM `{{ params.project_id }}.cl.vendors_v2` v
  LEFT JOIN UNNEST(rps) rps
  WHERE rps.is_latest
    OR rps IS NULL
), rps_orders_client_events_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl._rps_orders_client_events`
), rps_orders_devices_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl._rps_orders_devices`
), rps_oma_order_transitions_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl._rps_oma_order_transitions`
), rps_posmw_orders_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl._rps_posmw_orders`
), oma_restaurant_order_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.dl.oma_restaurant_order`
), oma_grocery_order_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.dl.oma_grocery_order`
), oma_logistics_service_order_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.dl.oma_logistics_service_order`
), oma_restaurant_orders AS (
  SELECT region
    , global_entity_id AS entity_id
    , UPPER(SPLIT(global_entity_id, '_')[OFFSET(1)]) AS country_iso
    , platform_global_key
    , platform_vendor_id AS vendor_code
    , CAST(vendor_id AS INT64) AS vendor_id
    , id AS order_id
    , platform_order_id AS order_code
    , logistics_delivery_id
    , state AS order_status
    , type AS business_model
    , JSON_EXTRACT_SCALAR(order_details, '$.comment') AS comment
    , created_date
    , created_at
    , expire_at
    , preordered_for
    , received_at
    , received_by_vendor_at
    , JSON_EXTRACT_SCALAR(order_details, '$.vendor.minutesToAccept') AS minutes_to_accept
    , CAST(JSON_EXTRACT_SCALAR(order_details, '$.test') AS BOOL) AS is_test_order
    , CAST(JSON_EXTRACT_SCALAR(order_details, '$.timings.preOrder') AS BOOL) AS is_preorder
    , PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(order_details, '$.timings.estimatedPickupAt')) AS estimated_pickup_at
    , PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(order_details, '$.timings.estimatedDeliverAt')) AS estimated_deliver_at
    , PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(order_details, '$.timings.originalEstimatedDeliverAt')) AS original_estimated_deliver_at
    , PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(order_details, '$.timings.pickedupAt')) AS picked_up_at
    , PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(order_details, '$.timings.deliveredAt')) AS delivered_at
    , PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(order_details, '$.timings.promisedFor')) AS promised_for
    , JSON_EXTRACT_SCALAR(order_details, '$.timings.preparationBufferMinutes') AS preparation_buffer_minutes
    , JSON_EXTRACT_SCALAR(order_details, '$.timings.preparationMinutes') AS preparation_minutes
    , PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(order_details, '$.timings.acceptedByVendorAt')) AS accepted_by_vendor_at
    , JSON_EXTRACT_SCALAR(order_details, '$.logistics.provider') AS logistics_provider
    , PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(order_details, '$.cancellation.timestamp')) AS cancelled_at
    , JSON_EXTRACT_SCALAR(order_details, '$.cancellation.reason') AS cancellation_reason
    , JSON_EXTRACT_SCALAR(order_details, '$.cancellation.stage') AS cancellation_stage
    , JSON_EXTRACT_SCALAR(order_details, '$.cancellation.causedBy') AS cancellation_owner
    , JSON_EXTRACT_SCALAR(order_details, '$.cancellation.source') AS cancellation_source
    , json2array(JSON_EXTRACT(order_details, '$.products')) AS products_extract
    , 'OMA' AS service
    , 'Restaurant' AS transmission_flow
  FROM oma_restaurant_order_dataset
), oma_grocery_orders AS (
  SELECT region
    , global_entity_id AS entity_id
    , UPPER(SPLIT(global_entity_id, '_')[OFFSET(1)]) AS country_iso
    , platform_global_key
    , platform_vendor_id AS vendor_code
    , NULL AS vendor_id
    , id AS order_id
    , platform_order_id AS order_code
    , logistics_delivery_id
    , state AS order_status
    , type AS business_model
    , JSON_EXTRACT_SCALAR(order_details, '$.comment') AS comment
    , created_date
    , created_at
    , expire_at
    , preordered_for
    , received_at
    , received_by_vendor_at
    , JSON_EXTRACT_SCALAR(order_details, '$.vendor.minutesToAccept') AS minutes_to_accept
    , CAST(JSON_EXTRACT_SCALAR(order_details, '$.test') AS BOOL) AS is_test_order
    , CAST(JSON_EXTRACT_SCALAR(order_details, '$.timings.preOrder') AS BOOL) AS is_preorder
    , PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(order_details, '$.timings.estimatedPickupAt')) AS estimated_pickup_at
    , PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(order_details, '$.timings.estimatedDeliverAt')) AS estimated_deliver_at
    , PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(order_details, '$.timings.originalEstimatedDeliverAt')) AS original_estimated_deliver_at
    , PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(order_details, '$.timings.pickedupAt')) AS picked_up_at
    , PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(order_details, '$.timings.deliveredAt')) AS delivered_at
    , PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(order_details, '$.timings.promisedFor')) AS promised_for
    , JSON_EXTRACT_SCALAR(order_details, '$.timings.preparationBufferMinutes') AS preparation_buffer_minutes
    , JSON_EXTRACT_SCALAR(order_details, '$.timings.preparationMinutes') AS preparation_minutes
    , PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(order_details, '$.timings.acceptedByVendorAt')) AS accepted_by_vendor_at
    , JSON_EXTRACT_SCALAR(order_details, '$.logistics.provider') AS logistics_provider
    , PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(order_details, '$.cancellation.timestamp')) AS cancelled_at
    , JSON_EXTRACT_SCALAR(order_details, '$.cancellation.reason') AS cancellation_reason
    , JSON_EXTRACT_SCALAR(order_details, '$.cancellation.stage') AS cancellation_stage
    , JSON_EXTRACT_SCALAR(order_details, '$.cancellation.causedBy') AS cancellation_owner
    , JSON_EXTRACT_SCALAR(order_details, '$.cancellation.source') AS cancellation_source
    , json2array(JSON_EXTRACT(order_details, '$.products')) AS products_extract
    , 'OMA' AS service
    , 'Grocery' AS transmission_flow
  FROM oma_grocery_order_dataset
), oma_logistics_service_orders AS (
  SELECT region
    , global_entity_id AS entity_id
    , UPPER(SPLIT(global_entity_id, '_')[OFFSET(1)]) AS country_iso
    , platform_global_key
    , platform_vendor_id AS vendor_code
    , CAST(NULL AS INT64) AS vendor_id
    , id AS order_id
    , platform_order_id AS order_code
    , logistics_delivery_id
    , state AS order_status
    , type AS business_model
    , JSON_EXTRACT_SCALAR(order_details, '$.comment') AS comment
    , created_date
    , created_at
    , expire_at
    , preordered_for
    , received_at
    , CAST(NULL AS TIMESTAMP) AS received_by_vendor_at
    , JSON_EXTRACT_SCALAR(order_details, '$.vendor.minutesToAccept') AS minutes_to_accept
    , CAST(JSON_EXTRACT_SCALAR(order_details, '$.test') AS BOOL) AS is_test_order
    , CAST(JSON_EXTRACT_SCALAR(order_details, '$.timings.preOrder') AS BOOL) AS is_preorder
    , PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(order_details, '$.timings.estimatedPickupAt')) AS estimated_pickup_at
    , PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(order_details, '$.timings.estimatedDeliverAt')) AS estimated_deliver_at
    , PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(order_details, '$.timings.originalEstimatedDeliverAt')) AS original_estimated_deliver_at
    , PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(order_details, '$.timings.pickedupAt')) AS picked_up_at
    , PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(order_details, '$.timings.deliveredAt')) AS delivered_at
    , PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(order_details, '$.timings.promisedFor')) AS promised_for
    , JSON_EXTRACT_SCALAR(order_details, '$.timings.preparationBufferMinutes') AS preparation_buffer_minutes
    , JSON_EXTRACT_SCALAR(order_details, '$.timings.preparationMinutes') AS preparation_minutes
    , PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(order_details, '$.timings.acceptedByVendorAt')) AS accepted_by_vendor_at
    , JSON_EXTRACT_SCALAR(order_details, '$.logistics.provider') AS logistics_provider
    , PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(order_details, '$.cancellation.timestamp')) AS cancelled_at
    , JSON_EXTRACT_SCALAR(order_details, '$.cancellation.reason') AS cancellation_reason
    , JSON_EXTRACT_SCALAR(order_details, '$.cancellation.stage') AS cancellation_stage
    , JSON_EXTRACT_SCALAR(order_details, '$.cancellation.causedBy') AS cancellation_owner
    , JSON_EXTRACT_SCALAR(order_details, '$.cancellation.source') AS cancellation_source
    , json2array(JSON_EXTRACT(order_details, '$.products')) AS products_extract
    , 'OMA' AS service
    , 'Logistics Service' AS transmission_flow
  FROM oma_logistics_service_order_dataset
), oma_orders_all_transmission_flows AS (
  SELECT *
  FROM (
    SELECT *
    FROM oma_restaurant_orders
    UNION ALL
    SELECT *
    FROM oma_grocery_orders
    UNION ALL
    SELECT *
    FROM oma_logistics_service_orders
  )
  WHERE NOT is_test_order
    AND (comment != 'test order. Do not Prepare' OR comment IS NULL)
    AND (cancellation_reason != 'TEST_ORDER' OR cancellation_reason IS NULL)
), oma_orderproducts AS (
  SELECT region
    , order_id
    , ARRAY_AGG(
        STRUCT(
          JSON_EXTRACT_SCALAR(p, '$.id') AS id
          , JSON_EXTRACT_SCALAR(p, '$.name') AS name
          , CAST(JSON_EXTRACT_SCALAR(p, '$.pricing.quantity') AS INT64) AS quantity
        )
      ) AS products
  FROM oma_orders_all_transmission_flows o
  CROSS JOIN UNNEST(products_extract) p
  GROUP BY 1,2
), oma_orders_all_transmission_flows_clean AS (
  SELECT o.* EXCEPT (cancellation_reason, products_extract)
    , CASE
        WHEN cancellation_reason = 'TRANSPORT_ACKNOWLEDGEMENT_TIMEOUT'
          THEN 'TECHNICAL_PROBLEM'
        WHEN cancellation_reason = 'PRODUCT_UNAVAILABLE'
          THEN 'ITEM_UNAVAILABLE'
        WHEN cancellation_reason = 'CUSTOMER_OUTSIDE_AREA'
          THEN 'OUTSIDE_DELIVERY_AREA'
        WHEN cancellation_reason = 'INCOMPLETE_ADDRESS'
          THEN 'ADDRESS_INCOMPLETE_MISSTATED'
        WHEN cancellation_reason = 'TOO_BUSY_KITCHEN'
          THEN 'TOO_BUSY'
        WHEN cancellation_reason = 'MENU_ACCOUNT_PROBLEM'
          THEN 'MENU_ACCOUNT_SETTINGS'
        ELSE cancellation_reason
      END AS cancellation_reason
    , op.products
  FROM oma_orders_all_transmission_flows o
  LEFT JOIN oma_orderproducts op ON o.order_id = op.order_id
    AND o.region = op.region
)
SELECT e.region
  , v.rps_global_key AS _rps_global_key
  , LOWER(o.country_iso) AS country_code
  , o.country_iso
  , STRUCT(
      e.entity_id AS id
      , e.entity_display_name AS display_name
      , e.brand_id
    ) AS entity
  , o.platform_global_key
  , o.order_id
  , posmwo.pos_order_id
  , v.vendor_id
  , v.vendor_code
  , o.order_code
  , o.logistics_delivery_id
  , IF(o.order_status = 'CANCELLED', 'cancelled', 'completed') AS order_status
  , ot.transitions
  , (SELECT stage FROM os.order_flow_stages fs WHERE o.order_status IN UNNEST(fs.states)) AS stage
  , CASE
      WHEN o.business_model = 'LOGISTICS_DELIVERY'
        THEN 'OWN_DELIVERY'
      WHEN o.business_model = 'VENDOR_DELIVERY'
        THEN 'VENDOR_DELIVERY'
      WHEN o.business_model = 'CUSTOMER_PICKUP'
        THEN 'PICKUP'
      ELSE NULL
    END AS delivery_type
  , o.comment
  , o.created_date
  , o.created_at
  , o.expire_at
  , o.preordered_for
  , o.received_at
  , o.received_by_vendor_at
  , o.minutes_to_accept
  , o.is_preorder
  , o.estimated_pickup_at
  , o.estimated_deliver_at
  , o.original_estimated_deliver_at
  , o.promised_for
  , o.preparation_buffer_minutes
  , o.preparation_minutes
  , (SELECT MIN(created_at) FROM ot.transitions t WHERE t.state = 'WAITING_FOR_TRANSPORT') AS waiting_for_transport_at
  , IF(o.business_model = 'LOGISTICS_DELIVERY',
      (SELECT MIN(created_at) FROM ot.transitions t WHERE t.state = 'ASSIGNED_TO_TRANSPORT'),
      o.received_at
    ) AS sending_to_vendor_at
  , (SELECT MIN(created_at) FROM ot.transitions t WHERE t.state = 'MODIFIED_BY_VENDOR') AS modified_by_vendor_at
  , (SELECT MIN(created_at) FROM ot.transitions t WHERE t.state = 'RECONCILIATION_STARTED') AS reconciliation_started_at
  , (SELECT MIN(created_at) FROM ot.transitions t WHERE t.state = 'RECONCILED_WITH_PLATFORM') AS reconciled_with_platform_at
  , (SELECT MIN(created_at) FROM ot.transitions t WHERE t.state = 'RECONCILED_WITH_TRANSPORT') AS reconciled_with_transport_at
  , o.accepted_by_vendor_at
  , (SELECT MIN(created_at) FROM ot.transitions t WHERE t.state = 'IMPLICITLY_ACCEPTED_BY_VENDOR') AS implicitly_accepted_by_vendor_at
  , (SELECT MIN(created_at) FROM ot.transitions t WHERE t.state = 'PREPARED') AS prepared_at
  , (SELECT MIN(created_at) FROM ot.transitions t WHERE t.state = 'NEAR_VENDOR') AS near_pickup_at
  , o.picked_up_at
  , (SELECT MIN(created_at) FROM ot.transitions t WHERE t.state = 'NEAR_CUSTOMER') AS near_dropoff_at
  , o.delivered_at
  , o.logistics_provider
  , o.cancelled_at
  , o.cancellation_stage
  , o.cancellation_source
  , o.service
  , o.transmission_flow
  , COALESCE(v.timezone, e.timezone) AS timezone
  , STRUCT(v.vendor_id AS id
      , v.vendor_code AS code
    ) AS vendor
  , v._operator_code
  , STRUCT(
      o.cancellation_reason AS reason
      , o.cancellation_owner AS owner
      , (o.cancellation_owner = 'VENDOR') AS is_cancelled_by_vendor
      , o.cancellation_stage AS stage
      , o.cancellation_source AS source
    ) AS cancellation
  , o.products
  , STRUCT(
      CASE
        WHEN posmwinth.integration_type = 'POS_AS_PRIMARY'
          THEN 'POS'
        WHEN d.client_name IS NOT NULL
          THEN d.client_name
        WHEN ev.order_id IS NOT NULL
          THEN ev.client_name
        WHEN posmwo.order_id IS NOT NULL
          THEN posmwo.client_name
        WHEN posmvmh.chain_mapped_id IS NOT NULL
          THEN 'POS'
        ELSE NULL
      END AS name
      , CASE
          WHEN posmwinth.integration_type = 'POS_AS_PRIMARY'
            THEN NULL
          WHEN d.client_version IS NOT NULL
            THEN d.client_version
          ELSE ev.version
        END AS version
      , CASE
          WHEN posmwinth.integration_type = 'POS_AS_PRIMARY'
            THEN NULL
          WHEN d.client_wrapper_type IS NOT NULL
            THEN d.client_wrapper_type
          ELSE ev.wrapper_type
        END AS wrapper_type
      , CASE
          WHEN posmwinth.integration_type = 'POS_AS_PRIMARY'
            THEN NULL
          WHEN d.client_wrapper_version IS NOT NULL
            THEN d.client_wrapper_version
          ELSE ev.wrapper_version
        END AS wrapper_version
      , CASE
          WHEN posmwinth.integration_type = 'POS_AS_PRIMARY'
            THEN 'DIRECT'
          WHEN (posmwinth.integration_type = 'POS_AS_SECONDARY') OR (posmwo.order_id IS NOT NULL AND ev.order_id IS NOT NULL)
            THEN 'INDIRECT'
          ELSE NULL
        END AS pos_integration_flow
      , posmvch.pos_chain_name AS pos_integration_name
      , CASE
          WHEN posmwinth.integration_type = 'POS_AS_PRIMARY'
            THEN 'POS'
          WHEN ev.order_id IS NOT NULL AND ev.client_name IN ('GODROID', 'GOWIN')
            THEN 'APPLICATION_CLIENT'
          WHEN posmwo.order_id IS NOT NULL
            THEN posmwo.client_name
          ELSE NULL
        END AS method
      , IF(posmwinth.integration_type = 'POS_AS_PRIMARY', NULL, d.os_name) AS os_name
      , IF(posmwinth.integration_type = 'POS_AS_PRIMARY', NULL, d.os_version) AS os_version
    ) AS client
  , STRUCT(
      d.device_id AS id
      , d.serial_number
      , d.mdm_service
      , d.manufacturer
      , d.model
      , d.is_ngt_device
    ) AS device
FROM oma_orders_all_transmission_flows_clean o
INNER JOIN entities e ON o.entity_id = e.entity_id
LEFT JOIN rps_orders_client_events_dataset ev ON o.order_id = ev.order_id
  AND e.region = ev.region
LEFT JOIN rps_oma_order_transitions_dataset ot ON o.order_id = ot.order_id
  AND e.region = ot.region
LEFT JOIN rps_orders_devices_dataset d ON o.order_id = d.order_id
  AND e.region = d.region
LEFT JOIN rps_posmw_orders_dataset posmwo ON o.order_id = posmwo.order_id
  AND e.region = posmwo.region
INNER JOIN vendors v ON o.vendor_code = v.vendor_code
  AND o.entity_id = v.entity_id
LEFT JOIN `{{ params.project_id }}.cl._posmw_vendor_integration_type_history` posmwinth ON v.vendor_id = posmwinth.vendor_id
  AND v.region = posmwinth.region
  AND (o.created_at >= posmwinth.updated_at AND o.created_at < posmwinth.next_updated_at)
LEFT JOIN `{{ params.project_id }}.cl._posmw_vendor_mapped_history` posmvmh ON v.vendor_id = posmvmh.vendor_id
  AND v.region = posmvmh.region
  AND (o.created_at >= posmvmh.updated_at AND o.created_at < posmvmh.next_updated_at)
LEFT JOIN `{{ params.project_id }}.cl._posmw_chain_mapped_history` posmvch ON v.region = posmvch.region
  AND posmvmh.chain_mapped_id = posmvch.chain_id
  AND (o.created_at >= posmvch.updated_at AND o.created_at < posmvch.next_updated_at)
CROSS JOIN `{{ params.project_id }}.cl._rps_order_status` AS os
