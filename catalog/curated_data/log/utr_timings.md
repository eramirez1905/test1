# Utr Timings

The following table has the components of the rider effective time as well as the costs per completed delivery. The at vendor and to vendor times have been calculated also considering the intravendor stacked deliveries and assigned these times per delivery. The to customer times have been calculated also considering the stacked deliveries and assigned per delivery. The timings have been split such that they depict the effective timings the rider has spent on that delivery.

Please note: The logic to calculate `idle_time`, `delivery_costs` and `delivery_costs_eur` are subject to change in the future.

| Column | Type | Description |
| :--- | :--- | :--- |
| country_code | `STRING`| A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2. |
| country_name | `STRING`| The name of the country in English. |
| city_id | `INTEGER`| The numeric identifier assigned to the city. This id is only unique within the same country |
| name | `STRING`| Name of the city in English. |
| zone_id | `INTEGER`| The numeric identifier assigned to the dropoff zone. This id is only unique within the same country. |
| zone_name | `STRING`| The name assigned to the dropoff zone. |
| entity_id | `STRING`| The code of the local operational entity representing the country. This code is made out of two codes : the two-letter code of the venture, and the country iso. |
| entity_display_name | `STRING`| Display name of the local operational entity representing the country. |
| vertical_type | `STRING` | It describes the vertical type of a vendor. In other words, it describes types of goods that are delivered. This data comes from the delivery area service (DAS). |
| vehicle_bag | `STRING`| The combination of Hurrier vehicle name and bag used by the rider for that specific delivery. |
| created_date | `DATE` | The date when the order was created. |
| delivery_date | `DATE` | The date of the delivery. |
| created_at | `TIMESTAMP`| Timestamp when delivery_id was created in Hurrier. |
| timezone | `STRING` | The name of the timezone where the city is located. The timezone enables time conversion, from UTC to local time. |
| order_id | `INTEGER`| The identifier of the order generated within the logistics system. It is specific to the order within a country. |
| id | `INTEGER`| The numeric identifier assigned to the vendor within the logistics systems. This id is only unique within the same country |
| vendor_code | `STRING`| The alphanumeric identifier that is sent from the platform to the logistics systems |
| rider_id | `INTEGER`| Identifier of the rider used by Rooster |
| delivery_count | `STRING`| The number of deliveries in the intravendor stack. |
| intravendor_key | `STRING`| String indicating the delivery ids stacked within the same vendor. |
| is_preorder | `BOOLEAN`| Flag whether the order was preordered. |
| rider_notified_at | `TIMESTAMP`| Datetime when last rider was notified about the delivery on Roadrunner app |
| rider_accepted_at | `TIMESTAMP`| Datetime when the last rider accepted the order on Roadrunner app |
| rider_near_restaurant_at | `TIMESTAMP`| Datetime when rider was 150 meters away from the restaurant. Timestamp triggered automatically by Hurrier |
| rider_picked_up_at | `TIMESTAMP`| Datetime when rider picked up the food. Rider has to click picked up to trigger this status |
| rider_near_customer_at | `TIMESTAMP`| Datetime when rider was 150 meters away from the customer. Timestamp triggered automatically by Hurrier |
| rider_dropped_off_at | `TIMESTAMP`| Datetime when rider delivered the food to the customer. Rider has to click dropped off to trigger this status |
| reaction_time | `FLOAT` | The time between rider notification and rider acceptance. In case multiple riders accepted the order, the last event is taken. |
| to_vendor_time | `FLOAT` | The time in seconds between rider near restaurant and rider acceptance. In case of stacked, intravendor deliveries, the to_vendor_time is the max to vendor time divided by the total deliveries in the stack allocated equally to all the deliveries in the intravendor stack.  |
| at_vendor_time | `FLOAT` | The time in seconds between rider picked up and rider near restaurant. In case of stacked, intravendor deliveries, the at_vendor_time is the max at vendor time divided by the total deliveries in the stack allocated equally to all the deliveries in the intravendor stack. |
| to_customer_time | `FLOAT` | The time in seconds between rider near customer and rider picked up. In case of stacked deliveries, the time for the first completed delivery is the time between rider near customer and rider picked up, the to_customer_time for the second completed delivery is the time between the first completed delivery and the second rider near customer and so on. |
| at_customer_time | `FLOAT` | The time in seconds between rider dropoff and rider near customer. |
| idle_time | `FLOAT` | The time rider is working but not touching any delivery assigned to the delivery. Total idle time is equally assigned to each delivery for that country and day. |
| rider_effective_time | `FLOAT` | The total time spent by rider on a delivery calculated as: `reaction_time + to_vendor_time + at_vendor_time + to_customer_time + at_customer_time`.  |
| delivery_costs | `FLOAT` | The total weekly rider costs obtained by summing the weekly costs by city and repartitioning them per delivery based on the total time spent on that single delivery including the `idle_time`. In local Currency.  |
| delivery_costs_eur | `FLOAT` | The total weekly rider costs obtained by summing the weekly costs by city and repartitioning them per delivery based on the total time spent on that single delivery including the `idle_time`. In Euros.  |
