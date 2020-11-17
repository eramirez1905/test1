CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.cancellations_report`
PARTITION BY report_date AS
WITH orders_dataset AS (
  SELECT o.country_code
    , o.city_id
    , entity.display_name AS entity_display_name
    , o.order_id
    , o.order_status
    , o.vendor.vertical_type
    , FORMAT_DATE('%G-%V', COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(o.created_at, o.timezone))) AS order_creation_week
    , COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(o.created_at, o.timezone)) AS order_creation_date
    , TIME_TRUNC(CAST(DATETIME(o.created_at, o.timezone) AS TIME), HOUR) AS order_creation_time_local
    , o.is_preorder
    , vendor.vendor_code
    , v.vendor_name
    , d.rider_near_restaurant_at AS near_pickup_at
    , d.rider_near_customer_at AS near_dropoff_at
    , d.rider_picked_up_at AS picked_up_at
    , o.sent_to_vendor_at AS order_sent_at
    , o.vendor_accepted_at AS order_received_at
    , FORMAT_DATE('%G-%V', DATE(t.created_at, o.timezone)) AS transition_creation_week
    , DATE(t.created_at, o.timezone) AS transition_creation_date
    , TIME_TRUNC(CAST(DATETIME(t.created_at, o.timezone) AS TIME), HOUR) AS transition_creation_time_local
    , IF(cancellation.source = 'api', TRUE, FALSE) AS is_system_cancelled
    , IF(cancellation.source = 'issue_service', TRUE, FALSE) AS is_issue_service_cancelled
    , IF(cancellation.source = 'dispatcher', TRUE, FALSE) AS is_dispatcher_cancelled
    , IF(cancellation.source = 'order_state_machine', TRUE, FALSE) AS is_order_state_machine_cancelled
    , IF(cancellation.source = 'delivery_state_machine', TRUE, FALSE) AS is_delivery_state_machine_cancelled
    , cancellation.reason AS cancellation_reason
    , CASE
        WHEN TIMESTAMP_DIFF(t.created_at, o.sent_to_vendor_at, SECOND) / 60 >= 15 AND o.vendor_accepted_at IS NULL AND o.sent_to_vendor_at IS NOT NULL
          THEN 1
        ELSE NULL
      END AS order_expired
    , CASE
        WHEN o.is_preorder IS FALSE AND o.order_status = 'cancelled' AND TIMESTAMP_DIFF(t.created_at, o.created_at, SECOND) / 60 < 180
          THEN TIMESTAMP_DIFF(t.created_at, o.created_at, SECOND) / 60
        ELSE NULL
      END AS time_to_cancel
  FROM `{{ params.project_id }}.cl.orders` o
  LEFT JOIN UNNEST(deliveries) d
  LEFT JOIN UNNEST(d.transitions) t ON t.state = 'cancelled'
  LEFT JOIN `{{ params.project_id }}.cl.vendors` v ON o.country_code = v.country_code
    AND vendor.id = v.vendor_id
  WHERE o.created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 8 WEEK)
)
SELECT o.country_code
  , co.country_name
  , o.entity_display_name
  , ci.name AS city_name
  , COALESCE(transition_creation_week, order_creation_week) AS report_week
  , COALESCE(transition_creation_date, order_creation_date) AS report_date
  , CAST(COALESCE(transition_creation_time_local, order_creation_time_local) AS STRING) AS report_time
  , o.is_preorder
  , o.vendor_code
  , o.vendor_name
  , o.vertical_type
  , COUNT(DISTINCT(IF(o.order_status = 'completed', o.order_id, NULL))) AS successful_orders
  , COUNT(DISTINCT(IF(o.order_status = 'cancelled', o.order_id, NULL))) AS cancelled_orders
  , SUM(time_to_cancel) AS time_to_cancel_sum
  , SUM(IF(is_dispatcher_cancelled AND order_expired IS NULL, time_to_cancel, NULL)) AS time_to_cancel_sum_m
  , COUNT(DISTINCT(IF(is_dispatcher_cancelled AND order_expired IS NULL, order_id, NULL))) AS time_to_cancel_count_m
  , SUM(IF(is_system_cancelled AND order_expired IS NULL, time_to_cancel, NULL)) AS time_to_cancel_sum_s
  , COUNT(DISTINCT(IF(is_system_cancelled AND order_expired IS NULL, order_id, NULL))) AS time_to_cancel_count_s
  , SUM(IF(order_expired IS NOT NULL, time_to_cancel, NULL)) AS time_to_cancel_sum_e
  , COUNT(DISTINCT(IF(order_expired IS NOT NULL, order_id, NULL))) AS time_to_cancel_count_e
  , SUM(IF(is_issue_service_cancelled AND order_expired IS NULL, time_to_cancel, NULL)) AS time_to_cancel_sum_i
  , COUNT(DISTINCT(IF(is_issue_service_cancelled AND order_expired IS NULL, order_id, NULL))) AS time_to_cancel_count_i
  , COUNT(DISTINCT time_to_cancel) AS time_to_cancel_count
  , COUNT(DISTINCT(IF(o.order_status = 'cancelled' AND is_system_cancelled AND order_expired IS NULL, o.order_id, NULL))) AS system_cancelled
  , COUNT(DISTINCT(IF(o.order_status = 'cancelled' AND is_dispatcher_cancelled AND order_expired IS NULL, o.order_id, NULL))) AS manual_cancelled
  , COUNT(DISTINCT(IF(o.order_status = 'cancelled' AND is_issue_service_cancelled AND order_expired IS NULL, o.order_id, NULL))) AS issue_service_cancelled
  , COUNT(DISTINCT(IF(o.order_status = 'cancelled' AND is_order_state_machine_cancelled AND order_expired IS NULL, o.order_id, NULL))) AS order_state_machine_cancelled
  , COUNT(DISTINCT(IF(o.order_status = 'cancelled' AND is_delivery_state_machine_cancelled AND order_expired IS NULL, o.order_id, NULL))) AS delivery_state_machine_cancelled
  , COUNT(DISTINCT(IF(o.order_status = 'cancelled' AND order_expired IS NOT NULL, o.order_id, NULL))) AS cancelled_expired
  , COUNT(DISTINCT(IF(o.near_pickup_at IS NOT NULL AND o.order_status = 'cancelled', o.order_id, NULL))) AS cancelled_after_rider_at_restaurant
  , COUNT(DISTINCT(IF(o.near_dropoff_at IS NOT NULL AND o.order_status = 'cancelled', o.order_id, NULL))) AS cancelled_after_rider_at_customer
  , COUNT(DISTINCT(IF(o.order_sent_at IS NULL AND o.order_status = 'cancelled', o.order_id, NULL))) AS cancelled_before_sent
  , COUNT(DISTINCT(IF(o.order_sent_at IS NOT NULL AND o.order_status = 'cancelled', o.order_id, NULL))) AS cancelled_after_sent
  , COUNT(DISTINCT(IF(o.order_sent_at IS NOT NULL AND o.order_received_at IS NULL AND o.order_status = 'cancelled', o.order_id, NULL)))  AS cancelled_after_sent_before_vendor_accepted
  , COUNT(DISTINCT(IF(o.order_sent_at IS NOT NULL AND o.order_received_at IS NOT NULL AND o.order_status = 'cancelled', o.order_id, NULL))) AS cancelled_after_sent_after_vendor_accepted
  , COUNT(DISTINCT(IF(o.picked_up_at IS NOT NULL AND o.order_status = 'cancelled', o.order_id, NULL))) AS cancelled_after_picked_up
  , COUNT(DISTINCT(IF(cancellation_reason IN ('TRAFFIC_MANAGER_NO_RIDER', 'NO_COURIER') AND is_dispatcher_cancelled AND o.order_status = 'cancelled' AND order_expired IS NULL, o.order_id, NULL))) AS traffic_manager_no_rider
  , COUNT(DISTINCT(IF(cancellation_reason IN ('TRAFFIC_MANAGER_TECHNICAL_ISSUE', 'TECHNICAL_PROBLEM') AND is_dispatcher_cancelled AND o.order_status = 'cancelled' AND order_expired IS NULL, o.order_id, NULL))) AS traffic_manager_technical_issue
  , COUNT(DISTINCT(IF(cancellation_reason IN ('TRAFFIC_MANAGER_UNABLE_FIND_CUSTOMER', 'UNABLE_TO_FIND') AND is_dispatcher_cancelled AND o.order_status = 'cancelled' AND order_expired IS NULL, o.order_id, NULL))) AS traffic_manager_unable_find_customer
  , COUNT(DISTINCT(IF(cancellation_reason IN ('TRAFFIC_MANAGER_OUTSIDE_OF_SERVICE_HOURS', 'OUTSIDE_SERVICE_HOURS') AND is_dispatcher_cancelled AND o.order_status = 'cancelled' AND order_expired IS NULL, o.order_id, NULL))) AS traffic_manager_outside_of_service_hours
  , COUNT(DISTINCT(IF(cancellation_reason IN ('TRAFFIC_MANAGER_RIDER_ACCIDENT', 'COURIER_ACCIDENT') AND is_dispatcher_cancelled AND o.order_status = 'cancelled' AND order_expired IS NULL, o.order_id, NULL))) AS traffic_manager_rider_accident
  , COUNT(DISTINCT(IF(cancellation_reason IN ('TRAFFIC_MANAGER_WEATHER_ISSUE', 'BAD_WEATHER') AND is_dispatcher_cancelled AND o.order_status = 'cancelled' AND order_expired IS NULL, o.order_id, NULL))) AS traffic_manager_weather_issue
  , COUNT(DISTINCT(IF(cancellation_reason = 'TRAFFIC_MANAGER_CUSTOMER_OUTSIDE_AREA' AND is_dispatcher_cancelled AND o.order_status = 'cancelled' AND order_expired IS NULL, o.order_id, NULL))) AS traffic_manager_customer_outside_area
  , COUNT(DISTINCT(IF(cancellation_reason IN ('TRAFFIC_MANAGER_VENDOR_CLOSED', 'CLOSED') AND is_dispatcher_cancelled AND o.order_status = 'cancelled' AND order_expired IS NULL, o.order_id, NULL))) AS traffic_manager_vendor_closed
  , COUNT(DISTINCT(IF(cancellation_reason = 'TRAFFIC_MANAGER_INCORRECT_ADDRESS' AND is_dispatcher_cancelled AND o.order_status = 'cancelled' AND order_expired IS NULL, o.order_id, NULL))) AS traffic_manager_incorrect_address
  , COUNT(DISTINCT(IF(cancellation_reason IN ('TRAFFIC_MANAGER_OUTSIDE_DELIVERY_AREA', 'OUTSIDE_DELIVERY_AREA') AND is_dispatcher_cancelled AND o.order_status = 'cancelled' AND order_expired IS NULL, o.order_id, NULL))) AS traffic_manager_outside_delivery_area
  , COUNT(DISTINCT(IF(cancellation_reason = 'MISTAKE_ERROR' AND is_dispatcher_cancelled AND o.order_status = 'cancelled' AND order_expired IS NULL, o.order_id, NULL))) AS mistake_error
  , COUNT(DISTINCT(IF(cancellation_reason IN ('ADDRESS_INCOMPLETE_MISSTATED', 'ADDRESS_INCOMPLETE_ADDRESS') AND is_dispatcher_cancelled AND o.order_status = 'cancelled' AND order_expired IS NULL, o.order_id, NULL))) AS address_incomplete_missatated
  , COUNT(DISTINCT(IF(cancellation_reason = 'LATE_DELIVERY' AND is_dispatcher_cancelled AND o.order_status = 'cancelled' AND order_expired IS NULL, o.order_id, NULL))) AS late_delivery
  , COUNT(DISTINCT(IF(cancellation_reason = 'ORDER_MODIFICATION_NOT_POSSIBLE' AND is_dispatcher_cancelled AND o.order_status = 'cancelled' AND order_expired IS NULL, o.order_id, NULL))) AS order_modification_not_possible
  , COUNT(DISTINCT(IF(cancellation_reason = 'DUPLICATE_ORDER' AND is_dispatcher_cancelled AND o.order_status = 'cancelled' AND order_expired IS NULL, o.order_id, NULL))) AS duplicate_order
  , COUNT(DISTINCT(IF(cancellation_reason = 'FRAUD_PRANK' AND is_dispatcher_cancelled AND o.order_status = 'cancelled' AND order_expired IS NULL, o.order_id, NULL))) AS fraud_prank
  , COUNT(DISTINCT(IF(cancellation_reason = 'FOOD_QUALITY_SPILLAGE' AND is_dispatcher_cancelled AND o.order_status = 'cancelled' AND order_expired IS NULL, o.order_id, NULL))) AS food_quality_spillage
  , COUNT(DISTINCT(IF(cancellation_reason = 'UNABLE_TO_PAY' AND is_dispatcher_cancelled AND o.order_status = 'cancelled' AND order_expired IS NULL, o.order_id, NULL))) AS unable_to_pay
  , COUNT(DISTINCT(IF(cancellation_reason = 'NEVER_DELIVERED' AND is_dispatcher_cancelled AND o.order_status = 'cancelled' AND order_expired IS NULL, o.order_id, NULL))) AS never_delivered
  , COUNT(DISTINCT(IF(cancellation_reason IN ('WRONG_ORDER_ITEMS_DELIVERED_TRANSPORT', 'WRONG_ORDER_ITEMS_DELIVERED_VENDOR') AND is_dispatcher_cancelled AND o.order_status = 'cancelled' AND order_expired IS NULL, o.order_id, NULL))) AS wrong_order_items_delivered_vendor
  , COUNT(DISTINCT(IF(cancellation_reason = 'COURIER_UNREACHABLE' AND is_dispatcher_cancelled AND o.order_status = 'cancelled' AND order_expired IS NULL, o.order_id, NULL))) AS courrier_unreachable
  , COUNT(DISTINCT(IF(cancellation_reason = 'UNPROFESSIONAL_BEHAVIOUR' AND is_dispatcher_cancelled AND o.order_status = 'cancelled' AND order_expired IS NULL, o.order_id, NULL))) AS unprofessional_behaviour
FROM orders_dataset o
LEFT JOIN `{{ params.project_id }}.cl.countries` co ON o.country_code = co.country_code
LEFT JOIN UNNEST(cities) ci ON o.city_id = ci.id
-- removing darkstore pickers to not impact utr as they do not have orders, only working hours
WHERE o.country_code NOT LIKE '%dp%'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
