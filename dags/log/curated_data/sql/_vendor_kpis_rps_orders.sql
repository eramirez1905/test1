CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._vendor_kpis_rps_orders`
PARTITION BY created_date_local
CLUSTER BY entity_id, vendor_code AS
WITH rps_orders_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl.rps_orders`
)
SELECT DATE(created_at, timezone) AS created_date_local
  , DATETIME_TRUNC(DATETIME(created_at, timezone), HOUR) AS created_hour_local
  , entity.id AS entity_id
  , vendor.code AS vendor_code
  , COUNT(*) AS orders_ct
  , COUNTIF(order_status = 'completed') AS success_orders_ct
  , COUNTIF(order_status = 'cancelled') AS fail_orders_ct
  , COUNTIF(order_status = 'cancelled' AND cancellation.owner = 'VENDOR') AS fail_orders_vendor_ct
  , COUNTIF(order_status = 'cancelled' AND cancellation.owner = 'TRANSPORT') AS fail_orders_transport_ct
  , COUNTIF(order_status = 'cancelled' AND cancellation.owner = 'PLATFORM') AS fail_orders_platform_ct
  , COUNTIF(order_status = 'cancelled' AND cancellation.owner = 'CUSTOMER') AS fail_orders_customer_ct
  , COUNTIF(order_status = 'cancelled' AND cancellation.owner = 'FULFILLMENT') AS fail_orders_fulfillment_ct
  , COUNTIF(order_status = 'cancelled' AND cancellation.reason = 'UNREACHABLE') AS fail_orders_unreachable_ct
  , COUNTIF(order_status = 'cancelled' AND cancellation.reason = 'ITEM_UNAVAILABLE') AS fail_orders_item_unavailable_ct
  , COUNTIF(order_status = 'cancelled' AND cancellation.reason = 'NO_RESPONSE') AS fail_orders_no_response_ct
  , COUNTIF(order_status = 'cancelled' AND cancellation.reason = 'CLOSED') AS fail_orders_closed_ct
  , COUNTIF(device.mdm_service = 'SOTI' AND device.manufacturer IN('SUNMI', 'V2')) AS ngt_orders_ct
  , COUNTIF(client.name = 'GODROID') AS godroid_orders_ct
  , COUNTIF(client.name = 'GOWIN') AS gowin_orders_ct
  , COUNTIF(client.name = 'POS') AS pos_orders_ct
  , COUNTIF(client.name = 'POS' AND client.pos_integration_flow = 'DIRECT') AS pos_direct_orders_ct
  , COUNTIF(client.name = 'POS' AND client.pos_integration_flow = 'INDIRECT') AS pos_indirect_orders_ct
  , COUNTIF(client.name = 'PLATFORM_CLIENT') AS platform_client_orders_ct
  , COUNTIF(delivery_type = 'OWN_DELIVERY') AS od_orders_ct
  , COUNTIF(delivery_type = 'VENDOR_DELIVERY') AS vd_orders_ct
  , COUNTIF(delivery_type = 'PICKUP') AS pu_orders_ct
  , COUNTIF(is_preorder) AS preorders_ct
  , COUNTIF(NOT is_preorder) AS asap_orders_ct
  , SUM(timings.transmission_time) AS total_transmission_time
  , SUM(timings.response_time) AS total_response_time
  , COUNTIF(
      order_status = 'cancelled'
      AND sending_to_vendor_at IS NULL
      AND received_by_vendor_at IS NULL
      AND accepted_by_vendor_at IS NULL
      AND modified_by_vendor_at IS NULL
      AND picked_up_at IS NULL
      AND delivered_at IS NULL
      AND cancellation.owner = 'CUSTOMER'
    ) AS fail_orders_customer_before_sent_to_vendor_ct
  , COUNTIF(
      order_status = 'cancelled'
      AND sending_to_vendor_at IS NULL
      AND received_by_vendor_at IS NULL
      AND accepted_by_vendor_at IS NULL
      AND modified_by_vendor_at IS NULL
      AND picked_up_at IS NULL
      AND delivered_at IS NULL
      AND cancellation.owner = 'PLATFORM'
    ) AS fail_orders_platform_before_sent_to_vendor_ct
  , COUNTIF(sending_to_vendor_at IS NOT NULL) AS orders_sent_to_vendor_ct
  , COUNTIF(
      order_status = 'cancelled'
      AND sending_to_vendor_at IS NOT NULL
      AND received_by_vendor_at IS NULL
      AND accepted_by_vendor_at IS NULL
      AND modified_by_vendor_at IS NULL
      AND picked_up_at IS NULL
      AND delivered_at IS NULL
      AND cancellation.owner = 'CUSTOMER'
    ) AS fail_orders_customer_before_received_by_vendor_ct
  , COUNTIF(
      order_status = 'cancelled'
      AND sending_to_vendor_at IS NOT NULL
      AND received_by_vendor_at IS NULL
      AND accepted_by_vendor_at IS NULL
      AND modified_by_vendor_at IS NULL
      AND picked_up_at IS NULL
      AND delivered_at IS NULL
      AND cancellation.owner = 'PLATFORM'
    ) AS fail_orders_platform_before_received_by_vendor_ct
  , COUNTIF(received_by_vendor_at IS NOT NULL) AS orders_received_by_vendor_ct
  , COUNTIF(
      order_status = 'cancelled'
      AND sending_to_vendor_at IS NOT NULL
      AND accepted_by_vendor_at IS NULL
      AND modified_by_vendor_at IS NULL
      AND picked_up_at IS NULL
      AND delivered_at IS NULL
      AND (cancellation.source <> 'VENDOR_DEVICE' OR cancellation.source IS NULL)
      AND cancellation.owner = 'CUSTOMER'
    ) AS fail_orders_customer_before_vendor_response_ct
  , COUNTIF(
      order_status = 'cancelled'
      AND sending_to_vendor_at IS NOT NULL
      AND accepted_by_vendor_at IS NULL
      AND modified_by_vendor_at IS NULL
      AND picked_up_at IS NULL
      AND delivered_at IS NULL
      AND (cancellation.source <> 'VENDOR_DEVICE' OR cancellation.source IS NULL)
      AND cancellation.owner = 'PLATFORM'
    ) AS fail_orders_platform_before_vendor_response_ct
  , COUNTIF(accepted_by_vendor_at IS NOT NULL) AS orders_accepted_by_vendor_ct
  , COUNTIF(accepted_by_vendor_at IS NOT NULL AND timings.response_time < 60) AS orders_accepted_by_vendor_within_1_min_ct
  , COUNTIF(accepted_by_vendor_at IS NOT NULL AND timings.response_time >= 60) AS orders_accepted_by_vendor_over_1_min_ct
  , COUNTIF(order_status = 'cancelled' AND cancellation.source = 'VENDOR_DEVICE') AS orders_rejected_by_vendor_ct
  , COUNTIF(modified_by_vendor_at IS NOT NULL) AS orders_modified_by_vendor_ct
  , COUNTIF(delivery_type = 'OWN_DELIVERY'
        AND TIMESTAMP_DIFF(COALESCE(original_estimated_deliver_at, estimated_deliver_at), sending_to_vendor_at, MINUTE) < 20) AS od_orders_estimated_time_within_20_mins_ct
  , COUNTIF(delivery_type = 'OWN_DELIVERY'
        AND TIMESTAMP_DIFF(COALESCE(original_estimated_deliver_at, estimated_deliver_at), sending_to_vendor_at, MINUTE) BETWEEN 20 AND 35) AS od_orders_estimated_time_20_to_35_mins_ct
  , COUNTIF(delivery_type = 'OWN_DELIVERY'
        AND TIMESTAMP_DIFF(COALESCE(original_estimated_deliver_at, estimated_deliver_at), sending_to_vendor_at, MINUTE) > 35) AS od_orders_estimated_time_over_35_mins_ct
  , COUNTIF(delivery_type = 'OWN_DELIVERY' AND preparation_minutes < 8) AS od_orders_preparation_time_within_8_mins_ct
  , COUNTIF(delivery_type = 'OWN_DELIVERY' AND preparation_minutes BETWEEN 8 AND 20) AS od_orders_preparation_time_8_to_20_mins_ct
  , COUNTIF(delivery_type = 'OWN_DELIVERY' AND preparation_minutes > 20) AS od_orders_preparation_time_over_20_mins_ct
  , COUNTIF(picked_up_at IS NOT NULL AND TIMESTAMP_DIFF(picked_up_at, estimated_pickup_at, MINUTE) >= 2) AS orders_picked_up_late_2min_ct
  , COUNTIF(implicitly_accepted_by_vendor_at IS NOT NULL) AS orders_implicitly_accepted_by_vendor_ct
  , COUNTIF(received_by_vendor_at IS NOT NULL
                AND (
                  accepted_by_vendor_at IS NOT NULL
                  OR modified_by_vendor_at IS NOT NULL
                  OR (cancelled_at IS NOT NULL
                      AND cancellation.source = 'VENDOR_DEVICE'
                      AND picked_up_at IS NULL
                      AND delivered_at IS NULL
                    )
                )
                AND TIMESTAMP_DIFF(COALESCE(accepted_by_vendor_at, modified_by_vendor_at, cancelled_at), received_by_vendor_at, SECOND) >= 30
    ) AS orders_confirmed_by_vendor_above_30_sec_ct
  , COUNTIF(TIMESTAMP_DIFF(sending_to_vendor_at , received_by_vendor_at, SECOND) >= 3) AS orders_sent_to_vendor_over_3_sec_ct
  , COUNTIF(order_status = 'completed' AND delivered_at > COALESCE(original_estimated_deliver_at, estimated_deliver_at)) AS orders_completed_late_ct
FROM rps_orders_dataset
GROUP BY 1,2,3,4
