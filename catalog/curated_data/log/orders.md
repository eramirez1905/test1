# Orders

An order entity represents a single order coming through RPS to Logistics. It should 1-1 correspond to an order on the platform backend.

An order can be further split up into multiple deliveries:
 - in case multiple riders need to pick up an order due to size, it will be split into multiple deliveries with one being the primary for tracking information
 - in case of missing items, a redelivery can be created to make sure the time spent by rider for redelivery is taken into account for dispatching and payments

 The order state machine is very coarse, fined graned statuses such as near_pickup, picked_up etc are on the delivery level.

 The `order_id` is unique only within a `country_code`.

| Column | Type | Description |
| :--- | :--- | :--- |
| country_code | `STRING`| A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2. |
| region | `STRING`| The operational region in which the country is located. The operational regions are: Americas, Asia, Australia, Europe, MENA and Others. |
| created_date | `DATE` | N\A (Partition column) |
| city_id | `INTEGER`| The numeric identifier assigned to the city. This id is only unique within the same country |
| zone_id | `INTEGER`| The numeric identifier assigned to the dropoff zone. This id is only unique within the same country. |
| is_preorder | `BOOLEAN`| Flag whether the order was preordered. |
| order_id | `INTEGER`| The identifier of the order generated within the logistics system. It is specific to the order within a country. |
| platform_order_id | `INTEGER`| The identifier of the order that is sent from the platform (e.g. foodora) to the logistic systems. It is specific to the order within a country. **Will be deprecated on February 21st, 2020. Use `platform_order_code` instead** |
| platform_order_code | `STRING`| The identifier of the order that is sent from the platform (e.g. foodora) to the logistic systems. It is specific to the order within a country.v |
| platform | `STRING`| The name of the platform (e.g. talabat) on which the order was placed. |
| [entity](#entity) | `RECORD`| The code of the local operational entity. |
| created_at | `TIMESTAMP`| The date and time when the order created within the logistics system. |
| timezone | `STRING` | The name of the timezone where the city is located. The timezone enables time conversion, from UTC to local time. |
| order_placed_at | `TIMESTAMP`| The date and time when the order was placed by the customer on the platform (e.g. on foodora website). |
| estimated_prep_time | `INTEGER`| The estimated time it takes the restaurant to prepare the food. The time estimation comes from a prediction model |
| estimated_prep_buffer | `INTEGER`| The estimated food preparation time buffer relfects the confidence that the restaurant will actually be ready when at the estimated preparation time. The value is shown in minutes |
| cod_change_for | `INTEGER`| Optional field sent by platforms (e.g. foodora) to logistics systems, indicating the cash change the rider should have when delivering the order to the customer |
| cod_collect_at_dropoff | `INTEGER`| The amount of money the rider collects from the customer. If not null, indicates the order was paid in cash. |
| cod_pay_at_pickup | `INTEGER`| The amount of money the rider pays at the vendor in cash, to pay for the order. If not null, indicates the order was paid in cash. |
| delivery_fee | `INTEGER`| The amount of the delivery fee paid for the order, in local currency.v |
| dropoff_address_id | `INTEGER`| The numeric identifier assigned to the drop-off location. This id is not sent by the platforms (e.g. foodora), but is generated within the logistics systemv |
| online_tip | `INTEGER`| The amount of the tip left by the customer online, in local currency.v |
| vendor_accepted_at | `TIMESTAMP`| The date and time when the vendor accepted the order. |
| sent_to_vendor_at | `TIMESTAMP`| The date and time when the vendor was notified about a new order |
| original_scheduled_pickup_at | `TIMESTAMP`| The date and time when the rider is supposed to pick up the food at the vendor's, as originally scheduled on the basis of the estimated preparation time. |
| pickup_address_id | `INTEGER`| The numeric identifier assigned to the pick-up location. This id is not sent by the platforms (e.g. foodora), but is generated within the logistics system |
| promised_delivery_time | `TIMESTAMP`| The date and time the order is supposed to be delivered to the customer. |
| updated_scheduled_pickup_at | `TIMESTAMP`| The scheduled date and time at which the rider is supposed to pickup the order after he/she received feedback from the restaurant. |
| stacking_group | `STRING` | N\A |
| order_status | `STRING`| The last process stage of the order. The order can be pending, completed, cancelled, in_progress. |
| tags | `ARRAY<STRING>`| Additional information concerning the order, appearing as a list (e.g. halal) |
| order_value | `INTEGER`| The value of the order paid by the customer, in local currency. |
| capacity | `INTEGER`| The monetary order value in relation to the max_order_value of the city. Value of "100" represents 100% capacityv |
| vendor_order_number | `INTEGER`| The daily order count of orders from the same vendor |
| [deliveries](#deliveries) | `ARRAY<RECORD>` | N\A |
| [cancellation](#cancellation) | `RECORD` | N\A |
| [customer](#customer) | `RECORD` | Customer information. |
| [vendor](#vendor) | `RECORD` | N\A |
| [porygon](#porygon) | `ARRAY<RECORD>` | Records pertaining porygon drive time polygons information. |
| [timings](#order-timings) | `RECORD` | N\A |

## Entity

| Column | Type | Description |
| :--- | :--- | :--- |
| id | `STRING`| The code of the local operational entity representing the country. This code is made out of two codes : the two-letter code of the venture, and the country iso. |
| display_name | `STRING`| Display name of the local operational entity representing the country. |
| brand_id | `STRING`| The code of the global operational brand in the country. This code is made out the code of the venture. |

## Deliveries

 The `id` is unique only within a `country_code`.

| Column | Type | Description |
| :--- | :--- | :--- |
| id | `INTEGER`| Id identifying the delivery |
| rider_id | `INTEGER`| Identifier of the rider used by Rooster |
| city_id | `INTEGER`| Id used by logistics to identify the city of the delivery within a country |
| delivery_status | `STRING` | N\A |
| delivery_reason | `STRING` | The cause for adding a redelivery by dispatcher |
| delivery_distance | `INTEGER`| The distance between the starting point and the drop off location |
| pickup_distance_manhattan | `FLOAT`| Distance calculated from rider accepted location until rider near pickup location as in https://en.wiktionary.org/wiki/Manhattan_distance |
| pickup_distance_google | `FLOAT`| Distance calculated from rider accepted location until rider near pickup location based on google routes |
| dropoff_distance_manhattan | `FLOAT`| Distance calculated from rider left pickup location until rider near customer location as in https://en.wiktionary.org/wiki/Manhattan_distance |
| dropoff_distance_google | `FLOAT`| Distance calculated from rider left pickup location until rider near customer location based on google routes |
| pickup_distance_dte | `FLOAT`| Travelled distance from rider accepted location to pickup location calculated by the Drive Time Model after the order has been completed. |
| dropoff_distance_dte | `FLOAT`| Travelled distance from rider left pickup location to dropoff location calculated by the Drive Time Model after the order has been completed. In the case of stacked orders, takes the last dropoff location to current dropoff location. |
| [estimated_distance_and_time_dte](#estimated-distance-and-time-dte) | `<ARRAY>RECORD`| Estimated travelled distance and time from rider left pickup location to dropoff location for all available vehicle profiles in the country. It is calculated during order creation. In the case of stacked orders, takes the last dropoff location to current dropoff location. |
| stacked_deliveries | `INTEGER`| Number of deliveries that share the bag with the current one at pick up. The value takes into account both past and future deliveries. |
| is_stacked_intravendor | `BOOLEAN`| Flag indicating if at least one of the stacked deliveries is at the same vendor |
| [stacked_deliveries_details](#stacked-deliveries-details) | `ARRAY<RECORD>`| Records pertaining deliveries that are stacked with the current `delivery_id` (Measured at pick_up). |
| rider_dispatched_at | `TIMESTAMP`| Datetime when the last rider got assigned the order on Roadrunner app |
| rider_notified_at | `TIMESTAMP`| Datetime when last rider was notified about the delivery on Roadrunner app |
| rider_accepted_at | `TIMESTAMP`| Datetime when the last rider accepted the order on Roadrunner app |
| rider_near_restaurant_at | `TIMESTAMP`| Datetime when rider was 150 meters away from the restaurant. Timestamp triggered automatically by Hurrier |
| rider_picked_up_at | `TIMESTAMP`| Datetime when rider picked up the food. Rider has to click picked up to trigger this status |
| rider_near_customer_at | `TIMESTAMP`| Datetime when rider was 150 meters away from the customer. Timestamp triggered automatically by Hurrier |
| rider_dropped_off_at | `TIMESTAMP`| Datetime when rider delivered the food to the customer. Rider has to click dropped off to trigger this status |
| [auto_transition](#auto-transition) | `RECORD`| Auto transition records. |
| is_primary | `BOOLEAN`| It indicates, in case of multiple deliveries for one order, if the delivery is the first completed in Hurrier. |
| created_at | `TIMESTAMP`| Datetime when delivery_id was created in Hurrier |
| timezone | `STRING` | The name of the timezone where the city is located. The timezone enables time conversion, from UTC to local time. |
| is_redelivery | `BOOLEAN`| It indicates if the delivery is a redelivery, for example in case of spilled food, missed out item |
| [vehicle](#vehicle) | `RECORD` | Vehicle records. |
| pickup | `GEOGRAPHY`| Position where the rider clicked picked up |
| dropoff | `GEOGRAPHY`| Position where the rider clicked dropped off |
| accepted | `GEOGRAPHY` | Position where the rider clicked accepted |
| rider_starting_point_id | `INTEGER` | The id of the starting point of the courier who completed the delivery. |
| [transitions](#delivery-transitions) | `RECORD` | Transitions records. |
| [timings](#delivery-timings) | `RECORD` | Delivery timings records.|

## Cancellation

| Column | Type | Description |
| :--- | :--- | :--- |
| source | `STRING` | N\A |
| performed_by | `STRING`| The ID of the user who cancelled the order within logistics system. This ID is unique within a region (Asia, Europe, Americas) |
| reason | `STRING`| The reason selected by the dispatcher when cancelling the order. IMPORTANT there are only cancellation reasons for order cancelled within logistics system. If the order was cancelled by vendors, customers or Customer Care agents, there will not be a reason |

## Customer

| Column | Type | Description |
| :--- | :--- | :--- |
| location | `GEOGRAPHY`| Location of the customer sent by the platform (e.g. foodora). |

## Auto Transition

| Column | Type | Description |
| :--- | :--- | :--- |
| pickup | `BOOLEAN`| Flag indicating if status was automatically moved from `rider_near_restaurant_at` to `rider_picked_up_at`. This may happen due to gps problems. |
| dropoff | `BOOLEAN`| Flag indicating if status was automatically moved from `rider_near_customer_at` to `rider_dropped_off_at`. This may happen due to gps problems. |

## Vendor

| Column | Type | Description |
| :--- | :--- | :--- |
| id | `INTEGER`| The numeric identifier assigned to the vendor within the logistics systems. This id is only unique within the same country |
| code | `STRING`| The alphanumeric identifier that is sent from the platform to the logistics systems |
| name | `STRING`| Name shown to the customers or used by Customer Care to recognize the vendor. <br>Maximum length is 300 characters |
| location | `GEOGRAPHY`| Vendor location sent by the platform (e.g. foodora). |
| vertical_type | `STRING` | Describe the vertical type of a vendor. In other words, it describes types of goods that are delivered. This data comes from the delivery area service (DAS). |

## Porygon

| Column | Type | Description |
| :--- | :--- | :--- |
| vehicle_profile | `STRING`| A profile describes whether or not it's possible to route along a particular type of way. |
| drive_time_value | `INTEGER`| Time in minutes that a courier goes from vendor location to each of the vertices of a drive time polygon. |
| active_vehicle_profile | `STRING`| The active porygon vehicle profile for given vendor at the time of order creation. |

## Vehicle

| Column | Type | Description |
| :--- | :--- | :--- |
| id | `INTEGER` | The numeric identifier of the vehicle with which the delivery was made. |
| name | `STRING` | The name of the vehicle with which the delivery was made. |
| profile | `STRING` | The google profile used to estimate the driving times to the vendor and the customer. |
| bag_name | `STRING` | The name of the bag associated with the vehicle name. |
| vehicle_bag | `STRING` | The combination of vehicle name and bag_name. |

## Estimated Distance and Time DTE

| Column | Type | Description |
| :--- | :--- | :--- |
| vehicle_type | `STRING`| Vehicle type to which the estimated distance and time applies. |
| time | `FLOAT`| Estimated travelled time from rider left pickup location to dropoff location. |
| distance | `FLOAT`| Estimated travelled distance from rider left pickup location to dropoff location. |

## Stacked Deliveries Details

| Column | Type | Description |
| :--- | :--- | :--- |
| delivery_id | `INTEGER`| Id identifying the delivery which has shared the bag or will share the bag with the current one before the current one gets delivered. |

## Order Timings

All timings are in `seconds`. In case an order has multiple deliveries, the timings of the primary delivery are considered to calculate order timings.
Timings are generally not computed in case of  Auto transition (when gps is not triggered). However, `actual_delivery_time`, `promised_delivery_time` and estimation KPIs are always computed.

| Column | Type | Description |
| :--- | :--- | :--- |
| updated_prep_time | `INTEGER` | The updated estimated time it takes the restaurant to prepare the food. The rider informed us about this update through the application |
| hold_back_time | `INTEGER` | The time until the order was sent to the vendor. |
| vendor_reaction_time | `INTEGER` | The time it took the vendor to accept the order. |
| dispatching_time | `INTEGER` | The time between the first dispatch to the time the rider was notified. In case multiple rider were notified, the last event is taken. |
| rider_reaction_time | `INTEGER` | The time between rider notification and rider acceptance. In case multiple riders accepted the order, the last event is taken. |
| rider_accepting_time | `INTEGER` | The time between the first dispatch to the time the rider accepted the order. In case multiple riders accepted the order, the last event is taken. |
| to_vendor_time | `INTEGER` | The time between rider acceptance and arriving at the restaurant. This includes the time it took the rider to walk into the vendor. |
| expected_vendor_walk_in_time | `INTEGER` | Estimated time how long it takes the rider to walk into the vendor in seconds. **Will be deprecated on June 23rd, 2020. Use `estimated_walk_in_duration` instead** |
| estimated_walk_in_duration | `FLOAT`| Estimated time how long it takes the rider to walk into the vendor in seconds. |
| estimated_walk_out_duration | `FLOAT`| Estimated time how long it takes the rider to walk out of the vendor in seconds. |
| estimated_courier_delay | `INTEGER`|  Estimated quantile delay either provided by platforms or Hurrier calling TES while the order is being created in seconds. |
| estimated_driving_time | `INTEGER`|  Estimated drive time provided by platforms or Hurrier calling TES while the order is being created based on the straight line distance formula on a country level (pickup to dropoff location) in seconds. |
| rider_late | `INTEGER` | The time difference between rider arrival at vendor and the original scheduled pickup time.  |
| assumed_actual_preparation_time | `INTEGER` | The assumed preparation time calculated by distinguishing three cases: <ul><li><code>rider_late &lt;= 0</code> computed as <code>rider_picked_up_at - sent_to_vendor_at</code></li><li><code>rider_late > 0</code> and spends less than 5 minutes in the restaurant: computed as <code>estimated_prep_time</code></li><li><code>rider_late > 0</code> and spends more than 5 minutes in the restaurant: computed as <code>rider_picked_up_at - sent_to_vendor_at</code></li></ul>All the cases exclude preorder, in case of preorder we do not compute this KPI. We also do not compute this KPI if `rider_late` > 15 minutes. There are also outlier pre filters based on data science model which make this KPI computable only on a sub set of orders. |
| bag_time | `INTEGER` | The time the food has been in the bag, calculated as: <br>`IF rider_near_pickup <= original_scheduled_pickup_at`: `rider_dropped_off_at` - `rider_picked_up_at` <br>`IF rider_near_pickup > updated_scheduled_pickup_at` AND `at_vendor_time` < 5 minutes: `rider_dropped_off_at` - `updated_scheduled_pickup_at`<br>`IF rider_near_pickup > updated_scheduled_pickup_at` AND `at_vendor_time` > = 5 minutes: `rider_dropped_off_at` - `rider_picked_up_at`. |
| vendor_late | `INTEGER` | The delayed actual pick up time compared to the original scheduled pickup time in seconds when this is attributable to the vendor. It distinguishes several cases: <ul><li><code>If rider spends 5 or more minutes in the restaurant it is computed as `rider_picked_up_at` - `original_scheduled_pickup_time`</code></li><li><code> If rider spends less than 5 minutes in the restaurant it is computed as `rider_picked_up_at` - `rider_near_restaurant_at`. </code></li></ul> It is computed only if rider is no later than 10 minutes. | 
| at_vendor_time | `INTEGER` | The time difference between rider arrival at vendor and the rider picking up the food. |
| at_vendor_time_cleaned | `INTEGER` | The at_vendor_time started to compute from the time food should have been picked up. Not computed if rider is late > 10 to allow possible re cooking of food. |
| vendor_arriving_time | `INTEGER` | N/A |
| vendor_leaving_time | `INTEGER` | N/A |
| expected_vendor_walk_out_time | `INTEGER` | Estimated time how long it takes the rider to walk out of the vendor in seconds. **Will be deprecated on June 23rd, 2020. Use `estimated_walk_out_duration` instead** |
| to_customer_time | `INTEGER` | The time difference between rider arrival at customer and the pickup time. |
| customer_walk_in_time | `INTEGER` | Estimated time how long it takes the rider to walk in to the customer's building. |
| at_customer_time | `INTEGER` | The time difference between rider arrival at customer time and the time the rider completed the order. |
| customer_walk_out_time | `INTEGER` | Estimated time how long it takes the rider to walk out of the customer's building. |
| actual_delivery_time | `INTEGER` | The time it took to deliver the order. Measured from order creation until rider at customer. |
| promised_delivery_time | `INTEGER` | The time which was promised to the customer upon order placement. |
| order_delay | `INTEGER` | The time difference between actual delivery time and promised delivery time. |
| [zone_stats](#zone-stats) | `RECORD` | Record containing stats about the zone order is delivered in. |

## Zone Stats
| Column | Type | Description |
| :--- | :--- | :--- |
| mean_delay | `FLOAT` | Average lateness in minutes of an order placed at this time (Used by dashboard, das, dps). |

## Delivery Transitions

| Column | Type | Description |
| :--- | :--- | :--- |
| state | `STRING` | The state the delivery transitioned into. |
| rider_id | `INTEGER` | The ID of the rider who was linked to the transition. |
| created_at | `TIMESTAMP` | The time and date the transition happened. |
| geo_point | `GEOGRAPHY` | The location of the rider during this transition. |
| latest | `BOOLEAN` | Flag indicating which transition is the most recent. |
| [actions](#actions) | `RECORD` | Actions details.|
| dispatch_type | `STRING` | It indicates the mode of the dispatching. |
| undispatch_type | `STRING` | It indicates the mode of the undispatching. |
| event_type | `STRING` | It specifies the context of the transition such as `delivery_replacement`. |
| update_reason  | `STRING` | The explanation of why `delivery_status` has been updated. |
| estimated_pickup_arrival_at | `TIMESTAMP` | Estimated time when the courier arrives at the restaurant. |
| estimated_dropoff_arrival_at | `TIMESTAMP` | Estimated time when the courier arrives at the customer. |

## Actions

| Column | Type | Description |
| :--- | :--- | :--- |
| user_id | `INTEGER` | The ID of the user who was linked to the action if manual. |
| performed_by | `STRING` | Who performed the action, eg: `dispatcher`, `issue_service`. |
| reason | `STRING` | The cause why the action was performed. |
| issue_type | `STRING` | The identification of the issue related to the action perfomed by `issue_service`. |
| comment | `STRING` | Any additional comment to the `reason`. |

## Delivery Timings
Timings are generally not computed in case of  Auto transition (when gps is not triggered). However, `actual_delivery_time`, `promised_delivery_time` and estimation KPIs are always computed.

| Column | Type | Description |
| :--- | :--- | :--- |
| dispatching_time | `INTEGER` | The time between the first dispatch to the time the rider was notified. In case multiple rider were notified, the last event is taken. |
| rider_reaction_time | `INTEGER` | The time between rider notification and rider acceptance. In case multiple riders accepted the delivery, the last event is taken. |
| rider_accepting_time | `INTEGER` | The time between the first dispatch to the time the rider accepted the delivery. In case multiple riders accepted the delivery, the last event is taken. |
| to_vendor_time | `INTEGER` | The time between rider acceptance and arriving at the restaurant. |
| rider_late | `INTEGER` | The time difference between rider arrival at vendor and the original scheduled pickup time. |
| vendor_late | `INTEGER` | The delayed actual pick up time compared to the original scheduled pickup time in seconds when this is attributable to the vendor. It distinguishes several cases: <ul><li><code>If rider spends 5 or more minutes in the restaurant it is computed as `rider_picked_up_at` - `original_scheduled_pickup_time`</code></li><li><code> If rider spends less than 5 minutes in the restaurant it is computed as `rider_picked_up_at` - `rider_near_restaurant_at`. </code></li></ul> It is computed only if rider is no later than 10 minutes. |
| assumed_actual_preparation_time | `INTEGER` | The assumed preparation time calculated by distinguishing three cases: <ul><li><code>rider_late &lt;= 0</code> computed as <code>rider_picked_up_at - sent_to_vendor_at</code></li><li><code>rider_late > 0</code> and spends less than 5 minutes in the restaurant: computed as <code>estimated_prep_time</code></li><li><code>rider_late > 0</code> and spends more than 5 minutes in the restaurant: computed as <code>rider_picked_up_at - sent_to_vendor_at</code></li></ul>All the cases exclude preorder, in case of preorder we do not compute this KPI. We also do not compute this KPI if `rider_late` > 15 minutes. There are also outlier pre filters based on data science model which make this KPI computable only on a sub set of orders. |
| bag_time | `INTEGER` | The time the food has been in the bag, calculated as: <br>`IF rider_near_pickup <= original_scheduled_pickup_at`: `rider_dropped_off_at` - `rider_picked_up_at` <br>`IF rider_near_pickup > updated_scheduled_pickup_at` AND `at_vendor_time` < 5 minutes: `rider_dropped_off_at` - `updated_scheduled_pickup_at`<br>`IF rider_near_pickup > updated_scheduled_pickup_at` AND `at_vendor_time` > = 5 minutes: `rider_dropped_off_at` - `rider_picked_up_at`. |
| at_vendor_time | `INTEGER` | The time difference between rider arrival at vendor and the rider picking up the food. |
| at_vendor_time_cleaned | `INTEGER` | The at_vendor_time started to compute from the time food should have been picked up. Not computed if rider is late > 10 to allow possible re cooking of food. |
| to_customer_time | `INTEGER` | The time difference between rider arrival at customer and the pickup time. |
| customer_walk_in_time | `INTEGER` | Estimated time how long it takes the rider to walk in to the customer's building. |
| at_customer_time | `INTEGER` | The time difference between rider arrival at customer time and the time the rider completed the delivery. |
| customer_walk_out_time | `INTEGER` | Estimated time how long it takes the rider to walk out of the customer's building. |
| actual_delivery_time | `INTEGER` | The time it took to deliver the delivery. Measured from order creation until rider at customer. |
| delivery_delay | `INTEGER` | The time difference between actual delivery time and promised delivery time. |
