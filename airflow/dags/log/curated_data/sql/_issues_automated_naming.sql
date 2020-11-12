CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._issues_automated_naming` AS
SELECT 'CourierIssue' AS type
  , 'not_in_the_zone' AS category
  , 'Courier not in the zone' AS name
UNION ALL
SELECT 'DispatchIssue' AS type
  , 'not_accepted_within_x_minutes' AS category
  , 'Not accepted within x minutes' AS name
UNION ALL
SELECT 'DispatchIssue' AS type
  , 'no_courier_interaction' AS category
  , 'Accept Issue' AS name
UNION ALL
SELECT 'DispatchIssue' AS type
  , 'courier_decline' AS category
  , 'Decline Issue' AS name
UNION ALL
SELECT 'PickupIssue' AS type
  , 'idle_order' AS category
  , 'Courier not moving towards Pickup' AS name
UNION ALL
SELECT 'PickupIssue'  AS type
  , 'waiting' AS category
  , 'Waiting at Pickup' AS name
UNION ALL
SELECT 'DropoffIssue' AS type
  , 'waiting' AS category
  , 'Waiting at Dropoff' AS name
UNION ALL
SELECT 'DropoffIssue' AS type
  , 'idle_order' AS category
  , 'Courier not moving towards Dropoff' AS name
UNION ALL
SELECT 'DispatchIssue' AS type
  , 'order_clicked_through' AS category
  , 'Order clicked through' AS name
UNION ALL
SELECT 'PickupIssue' AS type
  , 'no_gps_and_delayed' AS category
  , 'Pickup - no gps and delayed' AS name
UNION ALL
SELECT 'DispatchIssue' AS type
  , 'no_couriers_in_required_zones' AS category
  , 'No couriers in required zones' AS name
UNION ALL
SELECT 'DropoffIssue' AS type
  , 'no_gps_and_delayed' AS category
  , 'Dropoff - no gps and delayed' AS name
UNION ALL
SELECT 'DispatchIssue' AS type
  , 'distance_is_too_long' AS category
  , 'Distance is too long' AS name
UNION ALL
SELECT 'DispatchIssue' AS type
  , 'no_courier_can_deliver_within_shift' AS category
  , 'No courier can deliver within shift' AS name
UNION ALL
SELECT 'DispatchIssue' AS type
  , 'no_courier_with_required_vehicles' AS category
  , 'No courier with required vehicles' AS name
UNION ALL
SELECT 'DispatchIssue' AS type
  , 'none_of_required_couriers_working' AS category
  , 'None of the required couriers working' AS name
UNION ALL
SELECT 'PickupIssue' AS type
  , 'late' AS category
  , 'Late to Pickup' AS name
UNION ALL
SELECT 'DispatchIssue' AS type
  , 'can_not_stack_with_other_job_in_route' AS category
  , 'Cannot stack with other job in route' AS name
UNION ALL
SELECT 'DispatchIssue' AS type
  , 'limit_for_route_size_exceeded' AS category
  , 'Limit for route size exceeded' AS name
UNION ALL
SELECT 'DispatchIssue' AS type
  , 'no_courier_with_required_skills' AS category
  , 'No courier with required skills' AS name
UNION ALL
SELECT 'DispatchIssue' AS type
  , 'no_vehicle_with_required_capacity' AS category
  , 'No vehicle with required capacity' AS name
UNION ALL
SELECT 'CourierIssue' AS type
  , 'fake_gps_usage' AS category
  , 'Courier Fake GPS Usage Detected' AS name
UNION ALL
SELECT 'CourierIssue' AS type
  , 'app_issue' AS category
  , 'App Issue' AS name
UNION ALL
SELECT 'CourierIssue' AS type
  , 'locate_customer_issue' AS category
  , 'Unable to locate customer' AS name
UNION ALL
SELECT 'CourierIssue' AS type
  , 'restaurant_issue' AS category
  , 'Restaurant issue' AS name
UNION ALL
SELECT 'CourierIssue' AS type
  , 'equipment_issue' AS category
  , 'Equipment issue' AS name
UNION ALL
SELECT 'CourierIssue' AS type
  , 'break_request' AS category
  , 'Break request' AS name
UNION ALL
SELECT 'DispatchIssue' AS type
  , 'unknown_reason' AS category
  , 'Unknown reason' AS name
UNION ALL
SELECT 'DispatchIssue' AS type
  , 'non_dispatchable_order' AS category
  , 'Non dispatchable order' AS name
UNION ALL
SELECT 'DispatchIssue' AS type
  , 'delivery_in_another_courier_route' AS category
  , 'Delivery in another courier route' AS name
UNION ALL
SELECT 'DispatchIssue' AS type
  , 'dispatched_to_ending_courier' AS category
  , 'Dispatched to an ending courier' AS name
UNION ALL
SELECT 'PickupIssue' AS type
  , 'eta_requested' AS category
  , 'Delay at restaurant. Awaiting courier feedback for updated pick up time' AS name
UNION ALL
SELECT 'PickupIssue' AS type
  , 'no_courier_interaction' AS category
  , 'Courier did not hit Picked up' AS name
UNION ALL
SELECT 'PickupIssue' AS type
  , 'bad_packaging' AS category
  , 'Bad packaging' AS name
UNION ALL
SELECT 'DropoffIssue' AS type
  , 'no_courier_interaction' AS category
  , 'Courier did not hit Dropped off' AS name
UNION ALL
SELECT 'DropoffIssue' AS type
  , 'late' AS category
  , 'Late to Dropoff' AS name
UNION ALL
SELECT 'Issue::DropoffWaiting' AS type
  , '' AS category
  , 'Waiting at Dropoff' AS name
UNION ALL
SELECT 'Issue::CourierLateForShift' AS type
  , '' AS category
  , 'Courier late for shift' AS name
UNION ALL
SELECT 'Issue::DispatchNoCourierInteraction' AS type
  , '' AS category
  , 'Accept Issue' AS name
UNION ALL
SELECT 'Issue::PickupNoGpsAndDelayed' AS type
  , '' AS category
  , 'Pickup - no gps and delayed' AS name
UNION ALL
SELECT 'Issue::DispatchCanNotStackWithOtherJobInRoute' AS type
  , '' AS category
  , 'Cannot stack with other job in route' AS name
UNION ALL
SELECT 'Issue::DispatchNoVehicleWithRequiredCapacity' AS type
  , '' AS category
  , 'No vehicle with required capacity' AS name
UNION ALL
SELECT 'Issue::DispatchUnknownReason' AS type
  , '' AS category
  , 'Unknown reason' AS name
UNION ALL
SELECT 'Issue::DispatchCourierDecline' AS type
  , '' AS category
  , 'Decline Issue' AS name
UNION ALL
SELECT 'Issue::CourierFakeGpsUsage' AS type
  , '' AS category
  , 'Courier Fake GPS Usage Detected' AS name
UNION ALL
SELECT 'Issue::DispatchOrderClickedThrough' AS type
  , '' AS category
  , 'Order clicked through' AS name
UNION ALL
SELECT 'Issue::DispatchNoneOfRequiredCouriersWorking' AS type
  , '' AS category
  , 'None of the required couriers working' AS name
UNION ALL
SELECT 'Issue::CourierGpsOffForXMinutes' AS type
  , '' AS category
  , 'Courier GPS off for X minutes' AS name
UNION ALL
SELECT 'Issue::DropoffNoCourierInteraction' AS type
  , '' AS category
  , 'Courier did not hit Dropped off' AS name
UNION ALL
SELECT 'Issue::DispatchDistanceIsTooLong' AS type
  , '' AS category
  , 'Distance is too long' AS name
UNION ALL
SELECT 'Issue::CourierNotInTheZone' AS type
  , '' AS category
  , 'Courier not in the zone' AS name
UNION ALL
SELECT 'Issue::DispatchNonDispatchableOrder' AS type
  , '' AS category
  , 'Non dispatchable order' AS name
UNION ALL
SELECT 'Issue::PickupNoCourierInteraction' AS type
  , '' AS category
  , 'Courier did not hit Picked up' AS name
UNION ALL
SELECT 'Issue::DispatchNonShowableIssue' AS type
  , '' AS category
  , 'Non showable Issue' AS name
UNION ALL
SELECT 'Issue::DispatchNotAcceptedWithinXMinutes' AS type
  , '' AS category
  , 'Not accepted within x minutes' AS name
UNION ALL
SELECT 'Issue::DropoffNoGpsAndDelayed' AS type
  , '' AS category
  , 'Dropoff - no gps and delayed' AS name
UNION ALL
SELECT 'Issue::DispatchLimitForRouteSizeExceeded' AS type
  , '' AS category
  , 'Limit for route size exceeded' AS name
UNION ALL
SELECT 'Issue::DispatchNoCourierInRequiredZones' AS type
  , '' AS category
  , 'No couriers in required zones' AS name
UNION ALL
SELECT 'Issue::DispatchNoCourierCanDeliverWithinShift' AS type
  , '' AS category
  , 'No courier can deliver within shift' AS name
UNION ALL
SELECT 'Issue::DispatchDeliveryInAnotherCourierRoute' AS type
  , '' AS category
  , 'Delivery in another courier route' AS name
UNION ALL
SELECT 'Issue::PickupLate' AS type
  , '' AS category
  , 'Late to Pickup' AS name
UNION ALL
SELECT 'Issue::DispatchNoCourierWithRequiredVehicles' AS type
  , '' AS category
  , 'No courier with required vehicles' AS name
UNION ALL
SELECT 'Issue::DispatchNoCourierWithRequiredSkills' AS type
  , '' AS category
  , 'No courier with required skills' AS name
UNION ALL
SELECT 'Issue::CourierIdle' AS type
  , '' AS category
  , 'Courier Idle' AS name
UNION ALL
SELECT 'Issue::PickupWaiting' AS type
  , '' AS category
  , 'Waiting at Pickup' AS name
