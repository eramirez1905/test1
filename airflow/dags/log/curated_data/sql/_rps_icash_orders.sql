CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._rps_icash_orders`
PARTITION BY created_date
CLUSTER BY region, order_id AS
WITH entities AS (
  SELECT country_iso
    , LOWER(country_iso) AS country_code
    , entity_id
    , timezone
    , display_name AS entity_display_name
    , brand_id
    , rps_platforms
  FROM `{{ params.project_id }}.cl.entities` c
  LEFT JOIN UNNEST(c.platforms) platform
  LEFT JOIN UNNEST(platform.rps_platforms) rps_platforms
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
  CROSS JOIN UNNEST(rps) rps
  WHERE rps.is_latest
), rps_orders_client_events_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl._rps_orders_client_events`
), rps_orders_devices_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl._rps_orders_devices`
), rps_posmw_orders_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl._rps_posmw_orders`
), icash_order_transitions AS (
  SELECT region
    , created_date
    , created_at
    , order_id
    , (SELECT is_own_delivery FROM o.is_own_delivery_list ORDER BY created_at LIMIT 1) AS is_own_delivery
    , ARRAY(SELECT AS STRUCT * EXCEPT(metadata) FROM o.transitions ORDER BY created_at) AS transitions
    , (SELECT state FROM o.transitions WHERE state NOT IN ('CANCELLED') ORDER BY created_at DESC LIMIT 1) AS last_transition
    , (SELECT MIN(created_at) FROM o.transitions WHERE state = 'RECEIVED') AS received_at
    , (SELECT MIN(created_at) FROM o.transitions WHERE state = 'ASSIGNED_TO_TRANSPORT') AS assigned_to_transport_at
    , (SELECT MIN(created_at) FROM o.transitions WHERE state = 'WAITING_FOR_TRANSPORT') AS waiting_for_transport_at
    , (SELECT MIN(created_at) FROM o.transitions WHERE state = 'SENDING_TO_VENDOR') AS sending_to_vendor_at
    , (SELECT MIN(created_at) FROM o.transitions WHERE state = 'RECEIVED_BY_VENDOR') AS received_by_vendor_at
    , (SELECT MIN(created_at) FROM o.transitions WHERE state = 'ACCEPTED_BY_VENDOR') AS accepted_by_vendor_at
    , (SELECT MIN(created_at) FROM o.transitions WHERE state = 'NEAR_PICKUP') AS near_pickup_at
    , (SELECT MIN(created_at) FROM o.transitions WHERE state = 'PICKED_UP') AS picked_up_at
    , (SELECT MIN(created_at) FROM o.transitions WHERE state = 'NEAR_DROPOFF') AS near_dropoff_at
    , (SELECT MIN(created_at) FROM o.transitions WHERE state = 'DELIVERED') AS delivered_at
    , (SELECT MIN(created_at) FROM o.transitions WHERE state = 'CANCELLED') AS cancelled_at
  FROM (
    SELECT region
      , order_id
      , ARRAY_AGG(STRUCT(created_at, is_own_delivery)) AS is_own_delivery_list
      , MIN(created_date) AS created_date
      , MIN(created_at) AS created_at
      , ARRAY_CONCAT_AGG(transitions) AS transitions
    FROM `{{ params.project_id }}.cl._rps_icash_order_transitions`
    WHERE ARRAY_LENGTH(transitions) > 0
    GROUP BY 1,2
  ) o
), icash_delivery_dataset AS (
  -- temporary solution due to duplicated data coming from product
  -- issue: orders (delivery_id) with more than one vendor (belongs_to)
  -- issue: same order (delivery_id) in more than one country (country_code) within the same region
  SELECT * EXCEPT(_row_number)
  FROM (
    SELECT *
      , ROW_NUMBER() OVER (PARTITION BY region, delivery_id ORDER BY created_at DESC) AS _row_number
    FROM `{{ params.project_id }}.dl.icash_delivery`
    WHERE NOT COALESCE((flags & 256 != 0), FALSE) -- test order
      AND (order_comment != 'test order. Do not Prepare' OR order_comment IS NULL) -- test order Talabat
  )
  WHERE _row_number = 1
), icash_orders AS (
  SELECT o.region
    , o.created_date
    , o.delivery_timestamp AS created_at
    , o.delivery_id AS order_id
    , o.external_id AS order_code
    , posmwo.pos_order_id
    , o.is_preorder
    , o.order_comment AS comment
    , STRUCT(
        e.entity_id AS id
        , e.entity_display_name AS display_name
        , e.brand_id
      ) AS entity
    , e.timezone
    , o.country_code
    , o.global_key AS _rps_global_key
    , (SELECT ds.state FROM UNNEST(os.delivery_states) ds WHERE ds.id = o.state) AS _delivery_state
    , (SELECT s.state FROM UNNEST(os.dispatch_states) s WHERE o.dispatch_state = s.id) AS _dispatch_state
    , o.reject_reason AS _reject_reason
    , o.delivery_timestamp AS _delivery_timestamp
    , o.delivered_to_pos_timestamp AS _delivered_to_pos_timestamp
    , o.transport_pickup_time AS _transport_pickup_time
    , o.deliver_at AS estimated_deliver_at
    , o.deliver_at_before_delay AS original_estimated_deliver_at
    , o.promised_time AS promised_for
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
    , STRUCT(
        v.vendor_id AS id
        , v.vendor_code AS code
      ) AS vendor
    , v._operator_code
  FROM icash_delivery_dataset o
  LEFT JOIN rps_orders_client_events_dataset ev ON o.delivery_id = ev.order_id
    AND o.region = ev.region
  LEFT JOIN rps_posmw_orders_dataset posmwo ON o.delivery_id = posmwo.order_id
    AND o.region = posmwo.region
  LEFT JOIN entities e ON o.country_code = e.country_code
    AND o.global_key = e.rps_platforms
  LEFT JOIN rps_orders_devices_dataset d ON o.delivery_id = d.order_id
    AND o.region = d.region
  INNER JOIN vendors v ON o.external_restaurant_id = v.vendor_code
    AND e.entity_id = v.entity_id
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
  LEFT JOIN `{{ params.project_id }}.dl.icash_delivery_platform` dp ON o.delivery_platform = dp.id
    AND o.region = dp.region
)
SELECT o.*
  , ot.transitions
  , ot.last_transition
  , ot.is_own_delivery
  , ot.received_at
  , ot.waiting_for_transport_at
  , ot.sending_to_vendor_at
  , ot.received_by_vendor_at
  , ot.accepted_by_vendor_at
  , ot.near_pickup_at
  , ot.picked_up_at
  , ot.near_dropoff_at
  , ot.delivered_at
  , ot.cancelled_at
FROM icash_orders o
LEFT JOIN icash_order_transitions ot ON o.order_id = ot.order_id
  AND o.region = ot.region
