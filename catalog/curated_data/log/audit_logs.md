# Audit Logs

This table contains the logs coming from the various logistics applications, currently dashboard (Hurrier), Rooster and Porygon.
All the logs (except when old preceeds the name of the column or when the log pertains to a deletion) contain the new information updated by the user.



| Column | Type | Description |
| :--- | :--- | :--- |
| country_code | `STRING`| A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2. |
| action | `STRING`| The denomination of the action performed. |
| created_at | `TIMESTAMP`| The date and time when action was performed. |
| log_id | `INTEGER`| The identifier of the log in the table. | 
| user_id | `INTEGER`| The identifier of the user who performed the action. |
| email | `STRING`| The email address of the user who performed the action. |
| roles | `ARRAY<STRING>`| The roles of the user who performed the action. |
| application | `STRING`| The application on which action was performed (Eg:Rooster). |
| created_date | `DATE`| The date when action was performed. |
| country_name | `STRING`| The name of the country in English. |
| timezone | `STRING`| The name of the timezone where the country is located. The timezone enables time conversion, from UTC to local time. |
| [hurrier](#hurrier) | `ARRAY<RECORD>`| Hurrier actions.|
| [rooster](#rooster) | `ARRAY<RECORD>`| Rooster actions. |
| [delivery_area](#delivery-area) | `ARRAY<RECORD>`| Delivery area actions. |

## Hurrier

| Column | Type | Description |
| :--- | :--- | :--- |
| order_code | `STRING`| The identifier of the order that is sent from the platform (e.g. foodora) to the logistic systems. It is specific to the order within a country. |
| [cancel_order](#cancel-order) | `RECORD` | Records pertaining to cancel_order action. |
| [manual_dispatch](#manual-dispatch) | `RECORD` |Records pertaining to manual_dispatch action. |
| [update_order](#update-order) | `RECORD` | Records pertaining to update_order action. |
| [manual_undispatch](#manual-undispatch) | `RECORD` | Records pertaining to manual_undispatch action. |
| [replace_delivery](#replace-delivery) | `RECORD` | Records pertaining to replace_delivery action. |
| [update_delivery_status](#update-delivery-status) | `RECORD` | Records pertaining to update_delivery_status action. |
| [create_shift](#create-shift) | `RECORD` | Records pertaining to create_shift action. |
| [deactivate_shift](#deactivate-shift) | `RECORD` | Records pertaining to deactivate_shift action. |
| [update_shift](#update-shift) | `RECORD` | Records pertaining to update_shift action. |
| [finish_ongoing_shift](#finish-ongoing-shift) | `RECORD` | Records pertaining to finish_ongoing_shift action. |
| [force_connect](#force-connect) | `RECORD` | Records pertaining to force_connect action. |
| [courier_break](#courier-break) | `RECORD` | Records pertaining to courier_break action. |
| [courier_temp_not_working](#courier-temp-not-working) | `RECORD` | Records pertaining to courier_temp_not_working action. |
| [dismiss_issue](#dismiss-issue) | `RECORD` | Records pertaining to dismiss_issue action. |
| [resolve_issue](#resolve-issue) | `RECORD` | Records pertaining to resolve_issue action. |
| delivery_time_updated | `STRING` | It indicates the new delivery time updated by the user. |
| send_to_vendor | `STRING` | Order was manually sent to vendor by user. |
| [update_courier_route](#update-courier-route) | `RECORD` | Records pertaining to update_courier_route action. |
| [courier_working](#courier-working) | `RECORD` | Records pertaining to courier_working action. |
| [extend_shift](#extend-shift) | `RECORD` | Records pertaining to extend_shift action. |
| [change_dropoff_address](#change-dropoff-address) | `RECORD` | Records pertaining to change_dropoff_address action. |
| [update_order_dispatchers_notes](#update-order-dispatchers-notes) | `RECORD` | Records pertaining to update_order_dispatchers_notes action. |
| [courier_updated](#courier-updated) | `RECORD` | Records pertaining to courier_updated action. |

### Cancel Order

| Column | Type | Description |
| :--- | :--- | :--- |
| reason | `STRING` | The reason indicated by the dispatcher for cancelling the order. |

### Manual Dispatch

| Column | Type | Description |
| :--- | :--- | :--- |
| status | `STRING` | The last process stage of the order. |
| order_id | `INTEGER` | The identifier of the order generated within the logistics system. It is specific to the order within a country. |
| rider_id | `INTEGER` | Identifier of the rider used by Hurrier. |

### Update Order

| Column | Type | Description |
| :--- | :--- | :--- |
| order_id | `INTEGER` |The identifier of the order generated within the logistics system. It is specific to the order within a country. |
| status | `STRING` | The last process stage of the order. |

### Manual Undispatch

| Column | Type | Description |
| :--- | :--- | :--- |
| old_status | `STRING` | Previous process stage of the order. |
| order_id | `INTEGER` | The identifier of the order generated within the logistics system. It is specific to the order within a country. |
| old_courier_manual_dispatched | `BOOLEAN` | Flag indicating if the previous courier was manually dispatched. |
| old_rider_id| `INTEGER` | Identifier of the rider used by Hurrier. |

### Replace Delivery

| Column | Type | Description |
| :--- | :--- | :--- |
| order_id | `INTEGER` |The identifier of the order generated within the logistics system. It is specific to the order within a country. |
| rider_id | `INTEGER` |Identifier of the rider used by Hurrier. |

### Update Delivery Status

| Column | Type | Description |
| :--- | :--- | :--- |
| order_id | `INTEGER` | The identifier of the order generated within the logistics system. It is specific to the order within a country. |
| status | `STRING` | The last process stage of the order. |

### Create Shift

| Column | Type | Description |
| :--- | :--- | :--- |
| shift_id | `STRING` |Id of the shift in Rooster. |
| rider_id | `INTEGER` | Identifier of the rider used by Rooster. |

### Deactivate Shift

| Column | Type | Description |
| :--- | :--- | :--- |
| shift_id | `STRING` |Id of the shift in Rooster. |

### Update Shift

| Column | Type | Description |
| :--- | :--- | :--- |
| shift_id | `STRING` | Id of the shift in Rooster. |
| tag | `STRING` | Application mode to shift. |
| state | `STRING` | state of the shift: evaluated, in discussion, published, pending, cancelled, no show. |
| starting_point_id | `INTEGER` | Id of starting point the shift is assigned to in Rooster. |
| created_by | `INTEGER` | Identifier of user who created the shift. |
| updated_by | `INTEGER` | Identifier of user who made an update on the shift. |

### Finish Ongoing Shift

| Column | Type | Description |
| :--- | :--- | :--- |
| shift_id | `STRING` |Id of the shift in Rooster. |
| rider_id | `INTEGER` | Identifier of the rider used by Rooster. |

### Force Connect

| Column | Type | Description |
| :--- | :--- | :--- |
| rider_id | `INTEGER` | Identifier of the rider used by Rooster. |
| rider_email | `STRING` | Email of the rider. |

### Courier Break

| Column | Type | Description |
| :--- | :--- | :--- |
| rider_id | `INTEGER` |Identifier of the rider used by Rooster. |
| courier_lat | `FLOAT` | Courier Latitude at the action. |
| courier_long | `FLOAT` | Courier Longitude at the action. |

### Courier Temp Not Working

| Column | Type | Description |
| :--- | :--- | :--- |
| rider_id | `INTEGER` |Identifier of the rider used by Rooster. |

### Dismiss Issue

| Column | Type | Description |
| :--- | :--- | :--- |
| issue_id | `INTEGER` | The numeric identifier of an issue within Hurrier. |
| delivery_id | `INTEGER` | The numeric identifier of a delivery that issue can be linked to. |
| show_on_watchlist | `BOOLEAN` | Flag whether the issue was shown to the dispatcher by the newly introduced service Issue Customization Engine ICE. Only issues that are customized in ICE and were pushed to the issue watchlist will show TRUE. |

### Resolve Issue

| Column | Type | Description |
| :--- | :--- | :--- |
| issue_id | `INTEGER` | The numeric identifier of an issue within Hurrier. |
| external_id | `STRING` | The alphanumeric identifier of an issue within issue service. |
| notes | `STRING` | Additional specifications on the issue_type and issue_category. |
| show_on_watchlist | `BOOLEAN` | Flag whether the issue was shown to the dispatcher by the newly introduced service Issue Customization Engine ICE. Only issues that are customized in ICE and were pushed to the issue watchlist will show TRUE. |
| category | `STRING` | The category of the issue. |

### Update Courier Route

| Column | Type | Description |
| :--- | :--- | :--- |
| rider_id | `INTEGER` | Identifier of the rider used by Rooster. |
| event_type | `STRING` | The specific event pertaining to the action (Eg:pickup) |
| delivery_id | `INTEGER` | The numeric identifier of a delivery that action can be linked to. |

### Courier Working

| Column | Type | Description |
| :--- | :--- | :--- |
| rider_id | `INTEGER` | Identifier of the rider used by Rooster. |

### Extend Shift

| Column | Type | Description |
| :--- | :--- | :--- |
| shift_id | `STRING` | Id of the shift in Rooster. |

### Change Dropoff Address

| Column | Type | Description |
| :--- | :--- | :--- |
| distance_difference | `STRING` | Distance difference between old dropoff address and new one. |
| latitude | `FLOAT` | The latitude of the new address. |
| longitude | `FLOAT` | The longitude of the new address. |

### Update Order Dispatchers Notes

| Column | Type | Description |
| :--- | :--- | :--- |
| status | `STRING` | The last process stage of the order. |
| note | `STRING` | The content of the note that has been updated by the dispatcher. |

### Courier Updated

| Column | Type | Description |
| :--- | :--- | :--- |
| old_email | `STRING` | Courier email. |
| old_name | `STRING` | Courier name. |
| old_user_id | `INTEGER` | Courier user_id. |
| old_is_active | `BOOLEAN` | If courier was active or not. |
| old_is_enabled | `BOOLEAN` | If courier was enabled or not. |
| old_has_bag | `BOOLEAN` | If courier had bag or not. |
| old_vehicle_id | `INTEGER` | Identifier of the vehicle associated to the courier. |

### Delete Unassigned Shift

| Column | Type | Description |
| :--- | :--- | :--- |
| tag | `STRING` | Application mode to shift. |
| state | `STRING` |state of the shift: evaluated, in discussion, published, pending, cancelled, no show. |
| created_by | `INTEGER` | Identifier of user who created the shift. |
| updated_by | `INTEGER` | Identifier of user who made an update on the shift. |

## Rooster

| Column | Type | Description |
| :--- | :--- | :--- |
| [delete_unassigned_shift](#delete-unassigned-shift) | `RECORD` | Records pertaining to delete_unassigned_shift action. |
| [create_unassigned_shift](#create-unassigned-shift) | `RECORD` | Records pertaining to create_unassigned_shift action. |
| [update_employee_starting_points](#update-employee-starting-points) | `RECORD` | Records pertaining to update_employee_starting_points action. |
| [update_shift](#update-shift) | `RECORD` | Records pertaining to update_shift action. |
| [publish_shift](#publish-shift) | `RECORD` | Records pertaining to publish_shift action. |
| [delete_shift](#delete-shift) | `RECORD` | Records pertaining to delete_shift action. |
| [update_unassigned_shift](#update-unassigned-shift) | `RECORD` | Records pertaining to update_unassigned_shift action. |
| [publish_unassigned_shift](#publish-unassigned-shift) | `RECORD` | Records pertaining to publish_unassigned_shift action. |
| [create_shift](#create-shift) | `RECORD` | Records pertaining to create_shift action. |
| [create_swap](#create-swap) | `RECORD` | Records pertaining to swap action. |

### Create Unassigned Shift

| Column | Type | Description |
| :--- | :--- | :--- |
| tag | `STRING` | Application mode to shift. |
| state | `STRING` | state of the shift: evaluated, in discussion, published, pending, cancelled, no show. |
| created_by | `INTEGER` | Identifier of user who created the shift. |
| slots | `INTEGER` |  Number of allocated open slots. |

### Update Employee Starting Points

| Column | Type | Description |
| :--- | :--- | :--- |
| rider_id | `INTEGER` |Identifier of the rider used by Rooster. |
| starting_points | `STRING` | Id of starting points the employee can be assigned in Rooster. |

### Publish Shift

| Column | Type | Description |
| :--- | :--- | :--- |
| tag | `STRING` | Application mode to shift. |
| state | `STRING` | state of the shift: evaluated, in discussion, published, pending, cancelled, no show. |
| starting_point_id | `INTEGER` | Id of starting point the shift is assigned to in Rooster. |
| created_by | `INTEGER` | Identifier of user who created the shift. |
| updated_by | `INTEGER` | Identifier of user who made an update on the shift. |

### Delete Shift

| Column | Type | Description |
| :--- | :--- | :--- |
| shift_id | `STRING` | Id of the deleted shift in Rooster. |
| tag | `STRING` | Application mode to shift. |
| state | `STRING` | state of the shift: evaluated, in discussion, published, pending, cancelled, no show. |
| starting_point_id | `INTEGER` | Id of starting point the shift is assigned to in Rooster. |
| created_by | `INTEGER` | Identifier of user who created the shift. |
| updated_by | `INTEGER` | Identifier of user who made an update on the shift. |
| start_at | `TIMESTAMP` | UTC timestamp indicating start of shift. |
| end_at | `TIMESTAMP` | UTC timestamp indicating end of shift. |
| rider_id | `INTEGER` | Identifier of the rider used by Rooster. |
| day_of_week | `STRING` | Day of week on which shift is planned. |
| unassigned_shift_id | `STRING` | Id of the unassigned shift from which the deleted shift was created. |
| timezone | `STRING` | Timezone of the city in which the shift is planned. |

### Update Unassigned Shift

| Column | Type | Description |
| :--- | :--- | :--- |
| tag | `STRING` | Application mode to shift. |
| state | `STRING` | state of the shift: evaluated, in discussion, published, pending, cancelled, no show. |
| slots | `INTEGER` | Number of allocated open slots. |
| starting_point_id | `INTEGER` |Id of starting point the shift is assigned to in Rooster. |
| created_by | `INTEGER` | Identifier of user who created the shift. |
| updated_by | `INTEGER` | Identifier of user who made an update on the shift. |

### Publish Unassigned Shift

| Column | Type | Description |
| :--- | :--- | :--- |
| unassigned_shift_id | `STRING` |Id of the unassigned shift in Rooster. |
| tag | `STRING` | Application mode to shift. |
| state | `STRING` | state of the shift: evaluated, in discussion, published, pending, cancelled, no show. |
| slots | `INTEGER` | Number of allocated open slots. |
| start_at | `TIMESTAMP` | UTC timestamp indicating start of shift. |
| end_at | `TIMESTAMP` | UTC timestamp indicating end of shift. |
| starting_point_id | `INTEGER` | Id of starting point the shift is assigned to in Rooster. |
| created_by | `INTEGER` | Identifier of user who created the shift. |
| updated_by | `INTEGER` | Identifier of user who made an update on the shift. |

### Create Shift

| Column | Type | Description |
| :--- | :--- | :--- |
| shift_id | `STRING` |Id of the shift in Rooster. |
| tag | `STRING` | Application mode to shift. |
| state | `STRING` | state of the shift: evaluated, in discussion, published, pending, cancelled, no show. |
| starting_point_id | `INTEGER` | Id of starting point the shift is assigned to in Rooster. |
| created_by | `INTEGER` | Identifier of user who created the shift. |
| updated_by | `INTEGER` | Identifier of user who made an update on the shift. |
| start_at | `TIMESTAMP` | UTC timestamp indicating start of shift. |
| end_at | `TIMESTAMP` | UTC timestamp indicating end of shift. |
| rider_id | `INTEGER` | Identifier of the rider used by Rooster. |
| day_of_week | `STRING` | Day of week on which shift is planned. |
| unassigned_shift_id | `STRING` | Id of the unassigned shift out of which the shift was created. |
| timezone | `STRING` | Timezone of the city in which the shift is planned. |

### Create Swap

| Column | Type | Description |
| :--- | :--- | :--- |
| swap_id | `STRING` |Id of the swap. |
| shift_id | `STRING` |Id of the shift swapped. |
| accepted_at | `TIMESTAMP` | UTC timestamp at which the shift intended for a swap has been accepted. |
| accepted_by | `INTEGER` | Identifier of user who accepted the shift intended for a swap. |

## Delivery Area 

| Column | Type | Description |
| :--- | :--- | :--- |
| [delivery_area_create](#delivery-area-create) | `RECORD` | Records pertaining to delivery_area_create action. |
| [delivery_area_modify_or_create](#delivery-area-modify-or-create) | `RECORD` | Records pertaining to delivery_area_modify_or_create action. |
| [delivery_area_delete](#delivery-area-delete) | `RECORD` | Records pertaining to delivery_area_delete action. |
| [event_create](#event-create) | `RECORD` | Records pertaining to event_create action. |
| [event_create_replace](#event-create-replace) | `RECORD` | Records pertaining to event_create_replace action. |
| [event_delete](#event-delete) | `RECORD` | Records pertaining to event_delete action. |
| [event_modify](#event-modify) | `RECORD` | Records pertaining to event_modify action. |
| [city_create](#city-create) | `RECORD` | Records pertaining to city_create action. |
| [city_modify](#city-modify) | `RECORD` | Records pertaining to city_modify action. |
| [city_delete](#city-delete) | `RECORD` | Records pertaining to city_delete action. |
| [starting_point_create](#starting-point-create) | `RECORD` | Records pertaining to starting_point_create action. |
| [staring_point_delete](#staring-point-delete) | `RECORD` | Records pertaining to staring_point_delete action. |
| [starting_point_modify](#starting-point-modify) | `RECORD` | Records pertaining to starting_point_modify action. |
| [starting_point_replace](#starting-point-replace) | `RECORD` | Records pertaining to starting_point_replace action. |
| [starting_point_suggest](#starting-point-suggest) | `RECORD` | Records pertaining to starting_point_suggest action. |
| [zone_create](#zone-create) | `RECORD` | Records pertaining to zone_create action. |
| [zone_delete](#zone-delete) | `RECORD` | Records pertaining to zone_delete action. |
| [zone_replace](#zone-replace) | `RECORD` | Records pertaining to zone_replace action. |
| [zone_suggest](#zone-suggest) | `RECORD` | Records pertaining to zone_suggest action. |
| [zone_optimize](#zone-optimize) | `RECORD` | Records pertaining to zone_optimize action. |


### Delivery Area Create

| Column | Type | Description |
| :--- | :--- | :--- |
| vendor_code | `STRING` | Identifier of the restaurant. |
| drive_times | `STRING` | Time in minutes if delivery area polygon is based on drive time. |
| shape | `STRING` | Polygon a customer needs to be located in for being able to order with vendor. |
| fee | `STRING` | Fee value a customer has to pay for delivery. |
| fee_type | `STRING` | Type of fee a customer has to pay for delivery (percentage or absolute). |
| minimum_value | `STRING` | Minimum value an order must reach for a customer being able to order. |
| delivery_time | `STRING` | Expected time of delivery. |
| created_at | `TIMESTAMP` | UTC timestamp indicating when the delivery area is created. |
| updated_at | `TIMESTAMP` | UTC timestamp indicating when the delivery area is updated. |
| id | `STRING` | Id of the delivery area event. |
| operation_type | `INTEGER`| It specifies the action on the delivery area. It can be `0 (created)`, `1 (updated)` or `2 (cancelled)`. |


### Delivery Area Modify Or Create

| Column | Type | Description |
| :--- | :--- | :--- |
| vendor_code | `STRING` | Identifier of the restaurant. |
| drive_times | `STRING` | Time in minutes if delivery area polygon is based on drive time. |
| shape | `STRING` | Polygon a customer needs to be located in for being able to order with vendor. |
| fee | `STRING` | Fee value a customer has to pay for delivery. |
| fee_type | `STRING` | Type of fee a customer has to pay for delivery (percentage or absolute). |
| minimum_value | `STRING` | Minimum value an order must reach for a customer being able to order. |
| delivery_time | `STRING` | Expected time of delivery. |
| created_at | `TIMESTAMP` | UTC timestamp indicating when the delivery area is created or modified. |
| updated_at | `TIMESTAMP` | UTC timestamp indicating when the delivery area is created or modified. |
| id | `STRING` | Id of the delivery area event. |
| operation_type | `INTEGER`| It specifies the action on the delivery area. It can be `0 (created)`, `1 (updated)` or `2 (cancelled)`. |


### Delivery Area Delete

| Column | Type | Description |
| :--- | :--- | :--- |
| vendor_code | `STRING` | Identifier of the restaurant. |
| drive_times | `STRING` | Time in minutes if delivery area polygon is based on drive time. |
| shape | `STRING` | Polygon a customer needs to be located in for being able to order with vendor. |
| fee | `STRING` | Fee value a customer has to pay for delivery. |
| fee_type | `STRING` | Type of fee a customer has to pay for delivery (percentage or absolute). |
| minimum_value | `STRING` | Minimum value an order must reach for a customer being able to order. |
| delivery_time | `STRING` | Expected time of delivery. |
| created_at | `TIMESTAMP` | UTC timestamp indicating when the delivery area is deleted. |
| updated_at | `TIMESTAMP` | UTC timestamp indicating when the delivery area is deleted. |
| id | `STRING` | Id of the delivery area event. |
| operation_type | `INTEGER`| It specifies the action on the delivery area. It can be `0 (created)`, `1 (updated)` or `2 (cancelled)`. |


### Event Create

| Column | Type | Description |
| :--- | :--- | :--- |
| event_id | `STRING` | Identifier of the event. |
| event_title | `STRING` | Title of the event.  |
| event_action | `STRING` | Action of the event (delay, shrink, close, lockdown). |
| is_active | `STRING` | Boolean if event is active or not. |
| tags | `STRING` | Tags/attributes of vendors to which event applies. |
| shape | `STRING` | Polygon a customer needs to be located in for taking effect of event action. |
| zone_id | `STRING` | Identifier of zone that event is linked to. |
| shape_sync | `STRING` | Boolean if event polygon is kept in sync with zone polygon. |
| shrink_value | `STRING` | The drive time value in minutes that we reduce delivery area polygons to.   |
| activation_threshold | `STRING` | The threshold in minutes that the delay needs to exceed for the event to be activated. |
| deactivation_threshold | `STRING` | The threshold in minutes that the delay needs to fall below for the event to be activated. |


### Event Create Replace

| Column | Type | Description |
| :--- | :--- | :--- |
| event_id | `STRING` | Identifier of the event. |
| event_title | `STRING` | Title of the event. |
| event_action | `STRING` | Action of the event (delay, shrink, close, lockdown). |
| is_active | `STRING` | Boolean if event is active or not. |
| tags | `STRING` | Tags/attributes of vendors to which event applies. |
| shape | `STRING` | Polygon a customer needs to be located in for taking effect of event action. |
| zone_id | `STRING` | Identifier of zone that event is linked to. |
| shape_sync | `STRING` | Boolean if event polygon is kept in sync with zone polygon. |
| shrink_value | `STRING` | The drive time value in minutes that we reduce delivery area polygons to. |
| activation_threshold | `STRING` | The threshold in minutes that the delay needs to exceed for the event to be activated. |
| deactivation_threshold | `STRING` | The threshold in minutes that the delay needs to fall below for the event to be activated. |


### Event Delete

| Column | Type | Description |
| :--- | :--- | :--- |
| event_id | `STRING` | Identifier of the event. |
| event_title | `STRING` | Title of the event. |
| event_action | `STRING` | Action of the event (delay, shrink, close, lockdown). |
| is_active | `STRING` | Boolean if event is active or not. |
| tags | `STRING` | Tags/attributes of vendors to which event applies. |
| shape | `STRING` | Polygon a customer needs to be located in for taking effect of event action. |
| zone_id | `STRING` | Identifier of zone that event is linked to. |
| shape_sync | `STRING` | Boolean if event polygon is kept in sync with zone polygon. |
| shrink_value | `STRING` | The drive time value in minutes that we reduce delivery area polygons to.  |
| activation_threshold | `STRING` | The threshold in minutes that the delay needs to exceed for the event to be activated. |
| deactivation_threshold | `STRING` | The threshold in minutes that the delay needs to fall below for the event to be activated. |


### Event Modify

| Column | Type | Description |
| :--- | :--- | :--- |
| event_id | `STRING` | Identifier of the event. |
| event_title | `STRING` | Title of the event. |
| event_action | `STRING` | Action of the event (delay, shrink, close, lockdown). |
| is_active | `STRING` | Boolean if event is active or not. |
| tags | `STRING` | Tags/attributes of vendors to which event applies. |
| shape | `STRING` | Polygon a customer needs to be located in for taking effect of event action. |
| zone_id | `STRING` | Identifier of zone that event is linked to. |
| shape_sync | `STRING` | Boolean if event polygon is kept in sync with zone polygon. |
| shrink_value | `STRING` | The drive time value in minutes that we reduce delivery area polygons to.  |
| activation_threshold | `STRING` | The threshold in minutes that the delay needs to exceed for the event to be activated. |
| deactivation_threshold | `STRING` | The threshold in minutes that the delay needs to fall below for the event to be activated. |


### City Create

| Column | Type | Description |
| :--- | :--- | :--- |
| city_id | `STRING` | Identifier of the city. |
| city_name | `STRING` | The name of the city. |
| is_active | `STRING` | Boolean if city is active or not. |
| order_value_limit | `STRING` | The maximum order value which is considered to be 1 order. |


### City Modify

| Column | Type | Description |
| :--- | :--- | :--- |
| city_id | `STRING` | Identifier of the city. |
| city_name | `STRING` | The name of the city. |
| is_active | `STRING` | Boolean if city is active or not. |
| order_value_limit | `STRING` | The maximum order value which is considered to be 1 order. |


### City Delete

| Column | Type | Description |
| :--- | :--- | :--- |
| city_id | `STRING` | Identifier of the city. |
| city_name | `STRING` | The name of the city. |
| is_active | `STRING` | Boolean if city is active or not. |
| order_value_limit | `STRING` | The maximum order value which is considered to be 1 order. |


### Starting Point Create

| Column | Type | Description |
| :--- | :--- | :--- |
| starting_point_id | `STRING` | Identifier of the starting point. |
| zone_id | `STRING` | The id of the zone the starting point belongs to. |
| starting_point_name | `STRING` | The name of the starting point. |
| is_active | `STRING` | Boolean if starting point is active or not. |
| shape | `STRING` | Polygon a courier needs to be located in for being able to start his shift. |


### Starting Point Delete

| Column | Type | Description |
| :--- | :--- | :--- |
| starting_point_id | `STRING` | Identifier of the starting point. |
| zone_id | `STRING` | The id of the zone the starting point belongs to. |
| starting_point_name | `STRING` | The name of the starting point. |
| is_active | `STRING` | Boolean if starting point is active or not. |
| shape | `STRING` | Polygon a courier needs to be located in for being able to start his shift. |


### Starting Point Modify

| Column | Type | Description |
| :--- | :--- | :--- |
| starting_point_id | `STRING` | Identifier of the starting point. |
| zone_id | `STRING` | The id of the zone the starting point belongs to. |
| starting_point_name | `STRING` | The name of the starting point. |
| is_active | `STRING` | Boolean if starting point is active or not. |
| shape | `STRING` | Polygon a courier needs to be located in for being able to start his shift. |


### Starting Point Replace

| Column | Type | Description |
| :--- | :--- | :--- |
| starting_point_id | `STRING` | Identifier of the starting point. |
| zone_id | `STRING` | The id of the zone the starting point belongs to. |
| starting_point_name | `STRING` | The name of the starting point. |
| is_active | `STRING` | Boolean if starting point is active or not. |
| shape | `STRING` | Polygon a courier needs to be located in for being able to start his shift. |


### Starting Point Suggest

| Column | Type | Description |
| :--- | :--- | :--- |
| starting_point_id | `STRING` | Identifier of the starting point. |
| zone_id | `STRING` | The id of the zone the starting point belongs to. |
| starting_point_name | `STRING` | The name of the starting point. |
| is_active | `STRING` | Boolean if starting point is active or not. |
| shape | `STRING` | Polygon a courier needs to be located in for being able to start his shift. |


### Zone Create

| Column | Type | Description |
| :--- | :--- | :--- |
| zone_id | `STRING` | Identifier of the zone. |
| city_id | `STRING` | The id of the city the zone belongs to. |
| fleet_id | `STRING` | The id of the fleet that operates in the zone. |
| zone_name | `STRING` | The name of the zone. |
| is_active | `STRING` | Boolean if zone is active or not. |
| shape | `STRING` | Polygon that limits to which customer locations a courier can deliver. |


### Zone Delete

| Column | Type | Description |
| :--- | :--- | :--- |
| zone_id | `STRING` | Identifier of the zone. |
| city_id | `STRING` | The id of the city the zone belongs to. |
| fleet_id | `STRING` | The id of the fleet that operates in the zone. |
| zone_name | `STRING` | The name of the zone. |
| is_active | `STRING` | Boolean if zone is active or not. |
| shape | `STRING` | Polygon that limits to which customer locations a courier can deliver. |


### Zone Replace

| Column | Type | Description |
| :--- | :--- | :--- |
| zone_id | `STRING` | Identifier of the zone. |
| city_id | `STRING` | The id of the city the zone belongs to. |
| fleet_id | `STRING` | The id of the fleet that operates in the zone. |
| zone_name | `STRING` | The name of the zone. |
| is_active | `STRING` | Boolean if zone is active or not. |
| shape | `STRING` | Polygon that limits to which customer locations a courier can deliver. |


### Zone Suggest

| Column | Type | Description |
| :--- | :--- | :--- |
| zone_id | `STRING` | Identifier of the zone. |
| city_id | `STRING` | The id of the city the zone belongs to. |
| fleet_id | `STRING` | The id of the fleet that operates in the zone. |
| zone_name | `STRING` | The name of the zone. |
| is_active | `STRING` | Boolean if zone is active or not. |
| shape | `STRING` | Polygon that limits to which customer locations a courier can deliver. |


### Zone Optimize

| Column | Type | Description |
| :--- | :--- | :--- |
| filters | `STRING` | The vendor attribute filters that were used to narrow down the set of vendors. |
| shape | `STRING` | Polygon that limits to which customer locations a courier can deliver. |
| vehicle_profile | `STRING` | The OSRM vehicle profile that was used to create drive time polygons. |
| optimisations | `STRING` | The drive time and backend settings that were used to create delivery areas |
| cut_on_zone_border | `STRING` | Boolean whether delivery area polygons are cut at the borders of the zone a vendor is located in. |

## Arara

| Column | Type | Description |
| :--- | :--- | :--- |
| [applicant_move_to_status](#applicant-move-to-status) | `RECORD` | Records pertaining to applicant_move_to_status action. Logs who marked stages (steps) in the recruitment flow as it moves towards completion. |


### Applicant Move to Status
| Column | Type | Description |
| :--- | :--- | :--- |
| new_current_state | `STRING` | Shows the current state (step) of the recruitment. |
| old_current_state | `STRING` | Shows the current state (step) of the recruitment. |
