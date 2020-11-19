# lg_orders

Table of orders in logistics. Partitioned by created_date_utc, clustered by rdbms_id.

Partitioned by field `created_date_utc` with each
partition being a time interval of `DAY`

Clustered by field `rdbms_id`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid is unique and represents an order in logistics. Different from platform's, like pandora. |
| id | `INTEGER` |  |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| code | `STRING` |  |
| country_code | `STRING` |  |
| region | `STRING` |  |
| lg_city_uuid | `STRING` |  |
| lg_city_id | `INTEGER` |  |
| lg_zone_uuid | `STRING` |  |
| lg_zone_id | `INTEGER` |  |
| lg_dropoff_address_uuid | `STRING` |  |
| lg_dropoff_address_id | `INTEGER` |  |
| lg_pickup_address_uuid | `STRING` |  |
| lg_pickup_address_id | `INTEGER` |  |
| stacking_group | `STRING` |  |
| is_preorder | `BOOLEAN` |  |
| platform | `STRING` |  |
| [entity](#entity) | `RECORD` |  |
| order_status | `STRING` |  |
| tags | `ARRAY<STRING>` |  |
| capacity | `FLOAT` |  |
| vendor_order_number | `INTEGER` |  |
| customer_location_geo | `GEOGRAPHY` |  |
| estimated_prep_time_in_minutes | `INTEGER` |  |
| estimated_prep_buffer_in_minutes | `INTEGER` |  |
| updated_prep_time_in_seconds | `INTEGER` |  |
| hold_back_time_in_seconds | `INTEGER` |  |
| vendor_reaction_time_in_seconds | `INTEGER` |  |
| dispatching_time_in_seconds | `INTEGER` |  |
| rider_reaction_time_in_seconds | `INTEGER` |  |
| rider_accepting_time_in_seconds | `INTEGER` |  |
| to_vendor_time_in_seconds | `INTEGER` |  |
| expected_vendor_walk_in_time_in_seconds | `INTEGER` |  |
| estimated_walk_in_duration_in_seconds | `FLOAT` |  |
| estimated_walk_out_duration_in_seconds | `FLOAT` |  |
| estimated_courier_delay_in_seconds | `INTEGER` |  |
| estimated_driving_time_in_seconds | `INTEGER` |  |
| rider_late_in_seconds | `INTEGER` |  |
| vendor_late_in_seconds | `INTEGER` |  |
| assumed_actual_preparation_time_in_seconds | `INTEGER` |  |
| bag_time_in_seconds | `INTEGER` |  |
| at_vendor_time_in_seconds | `INTEGER` |  |
| at_vendor_time_cleaned_in_seconds | `INTEGER` |  |
| vendor_arriving_time_in_seconds | `INTEGER` |  |
| vendor_leaving_time_in_seconds | `INTEGER` |  |
| expected_vendor_walk_out_time_in_seconds | `INTEGER` |  |
| to_customer_time_in_seconds | `INTEGER` |  |
| customer_walk_in_time_in_seconds | `INTEGER` |  |
| at_customer_time_in_seconds | `INTEGER` |  |
| customer_walk_out_time_in_seconds | `INTEGER` |  |
| actual_delivery_time_in_seconds | `INTEGER` |  |
| promised_delivery_time_in_seconds | `INTEGER` |  |
| order_delay_in_seconds | `INTEGER` |  |
| mean_delay_in_zone_in_seconds | `FLOAT` |  |
| cod_change_for_local | `INTEGER` |  |
| cod_collect_at_dropoff_local | `INTEGER` |  |
| cod_pay_at_pickup_local | `INTEGER` |  |
| delivery_fee_local | `INTEGER` |  |
| online_tip_local | `INTEGER` |  |
| total_value_local | `INTEGER` |  |
| timezone | `STRING` |  |
| updated_scheduled_pickup_at_utc | `TIMESTAMP` |  |
| promised_delivery_at_utc | `TIMESTAMP` |  |
| vendor_accepted_at_utc | `TIMESTAMP` |  |
| sent_to_vendor_at_utc | `TIMESTAMP` |  |
| original_scheduled_pickup_at_utc | `TIMESTAMP` |  |
| order_placed_at_utc | `TIMESTAMP` |  |
| created_date_utc | `DATE` |  |
| created_at_utc | `TIMESTAMP` |  |
| [deliveries](#deliveries) | `ARRAY<RECORD>` |  |
| [porygon](#porygon) | `ARRAY<RECORD>` |  |
| [cancellation](#cancellation) | `RECORD` |  |
| [vendor](#vendor) | `RECORD` |  |

## entity

| Name | Type | Description |
| :--- | :--- | :---        |
| id | `STRING` |  |
| display_name | `STRING` |  |
| brand_id | `STRING` |  |

## deliveries

| Name | Type | Description |
| :--- | :--- | :---        |
| id | `INTEGER` |  |
| uuid | `STRING` |  |
| lg_rider_id | `INTEGER` |  |
| lg_rider_uuid | `STRING` |  |
| lg_rider_starting_point_id | `INTEGER` |  |
| lg_rider_starting_point_uuid | `STRING` |  |
| status | `STRING` |  |
| distance_in_meters | `INTEGER` |  |
| stacked_deliveries_count | `INTEGER` |  |
| is_pickup_auto_transition | `BOOLEAN` |  |
| is_dropoff_auto_transistion | `BOOLEAN` |  |
| is_primary | `BOOLEAN` |  |
| is_redelivery | `BOOLEAN` |  |
| is_stacked_intravendor | `BOOLEAN` |  |
| pickup_geo | `GEOGRAPHY` |  |
| dropoff_geo | `GEOGRAPHY` |  |
| accepted_geo | `GEOGRAPHY` |  |
| pickup_distance_manhattan_in_meters | `FLOAT` |  |
| pickup_distance_google_in_meters | `FLOAT` |  |
| dropoff_distance_manhattan_in_meters | `FLOAT` |  |
| dropoff_distance_google_in_meters | `FLOAT` |  |
| dispatching_time_in_seconds | `INTEGER` |  |
| rider_reaction_time_in_seconds | `INTEGER` |  |
| rider_accepting_time_in_seconds | `INTEGER` |  |
| to_vendor_time_in_seconds | `INTEGER` |  |
| vendor_arriving_time_in_seconds | `INTEGER` |  |
| vendor_leaving_time_in_seconds | `INTEGER` |  |
| rider_late_in_seconds | `INTEGER` |  |
| vendor_late_in_seconds | `INTEGER` |  |
| assumed_actual_preparation_time_in_seconds | `INTEGER` |  |
| bag_time_in_seconds | `INTEGER` |  |
| at_vendor_time_in_seconds | `INTEGER` |  |
| at_vendor_time_cleaned_in_seconds | `INTEGER` |  |
| to_customer_time_in_seconds | `INTEGER` |  |
| customer_walk_in_time_in_seconds | `INTEGER` |  |
| at_customer_time_in_seconds | `INTEGER` |  |
| customer_walk_out_time_in_seconds | `INTEGER` |  |
| actual_delivery_time_in_seconds | `INTEGER` |  |
| delivery_delay_in_seconds | `INTEGER` |  |
| rider_notified_at_utc | `TIMESTAMP` |  |
| rider_dispatched_at_utc | `TIMESTAMP` |  |
| rider_accepted_at_utc | `TIMESTAMP` |  |
| rider_near_restaurant_at_utc | `TIMESTAMP` |  |
| rider_picked_up_at_utc | `TIMESTAMP` |  |
| rider_near_customer_at_utc | `TIMESTAMP` |  |
| created_at_utc | `TIMESTAMP` |  |
| [vehicle](#vehicle) | `RECORD` |  |
| [stacked_deliveries](#stackeddeliveries) | `ARRAY<RECORD>` |  |
| [transitions](#transitions) | `ARRAY<RECORD>` |  |

## vehicle

| Name | Type | Description |
| :--- | :--- | :---        |
| id | `INTEGER` |  |
| name | `STRING` |  |
| profile | `STRING` |  |
| vehicle_bag | `STRING` |  |
| bag_name | `STRING` |  |

## stacked_deliveries

| Name | Type | Description |
| :--- | :--- | :---        |
| lg_delivery_id | `INTEGER` |  |
| lg_delivery_uuid | `STRING` |  |

## transitions

| Name | Type | Description |
| :--- | :--- | :---        |
| state | `STRING` |  |
| is_auto_transition | `BOOLEAN` |  |
| is_latest | `BOOLEAN` |  |
| is_first | `BOOLEAN` |  |
| point_geo | `GEOGRAPHY` |  |
| dispatch_type | `STRING` |  |
| undispatch_type | `STRING` |  |
| event_type | `STRING` |  |
| update_reason | `STRING` |  |
| [actions](#actions) | `RECORD` |  |
| estimated_pickup_arrival_at_utc | `TIMESTAMP` |  |
| estimated_dropoff_arrival_at_utc | `TIMESTAMP` |  |
| created_at_utc | `TIMESTAMP` |  |

## actions

| Name | Type | Description |
| :--- | :--- | :---        |
| lg_user_id | `INTEGER` |  |
| lg_user_uuid | `STRING` |  |
| performed_by_type | `STRING` |  |
| issue_type | `STRING` |  |
| reason | `STRING` |  |
| comment | `STRING` |  |

## porygon

| Name | Type | Description |
| :--- | :--- | :---        |
| vehicle_profile | `STRING` |  |
| drive_time_in_minutes | `INTEGER` |  |
| active_vehicle_profile | `STRING` |  |

## cancellation

| Name | Type | Description |
| :--- | :--- | :---        |
| performed_by_lg_user_id | `INTEGER` |  |
| source | `STRING` |  |
| reason | `STRING` |  |

## vendor

| Name | Type | Description |
| :--- | :--- | :---        |
| id | `INTEGER` |  |
| uuid | `STRING` |  |
| code | `STRING` |  |
| name | `STRING` |  |
| location_geo | `GEOGRAPHY` |  |
| vertical_type | `STRING` |  |
