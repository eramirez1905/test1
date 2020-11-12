# Delivery Areas Events

### Delivery Areas Events

| Column | Type | Description |
| :--- | :--- | :--- |
| country_code | `STRING`| A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2. |
| event_id | `STRING`| Identifier of the event in Porygon application. |
| active_from | `TIMESTAMP`| The datetime from when the attributes of the event start to be active. |
| active_to | `TIMESTAMP`| The datetime from when the attributes of the event stopped to be active. If the event is still active, the value is `NULL`. |
| operation_type | `STRING`| It specifies the action on the event. It can be `created`, `updated` or `cancelled`. |
| [city](#city) | `RECORD` | The city of the vendor within a country. |
| timezone | `INTEGER`| The timezone of the city in which the event takes place. |
| zone_id | `INTEGER`| The numeric identifier of the zone in which the event has taken place. If the event has been triggered manually, the zone is set to null. |
| transaction_id | `INTEGER`| The identifier created when event was created or updated. |
| end_transaction_id | `INTEGER`| The identifier created when event was updated. |
| start_at | `TIMESTAMP`| The time at which the activation or deactivation has started. |
| end_at | `TIMESTAMP`| The time at which the activation or deactivation has stopped. |
| duration | `INTEGER`| The duration of the activation or deactivation event, in seconds. |
| is_active | `BOOLEAN`| Flag indicating if the event is currently active or not. |
| action | `STRING`| The type of the event : closure, shrinkage, delay, lockdown. |
| value | `INTEGER`| In the case of shrinkages, the drive time defining the area of the polygon in which the restaurant delivers. |
| activation_threshold | `INTEGER`| The delay threshold above which an event is activated. |
| deactivation_threshold | `INTEGER`| The delay threshold under which an event is deactivated. |
| title | `STRING`| The title of the event. |
| is_shape_in_sync | `BOOLEAN`| Flag whether an event polygon is kept in sync with the polygon of a zone as referenced by zone_id. |
| [message](#message) | `ARRAY<RECORD>`| Event message in all languages. JSON format. |
| shape | `GEOGRAPHY`| The geometrical shape of the delivery area. |
| [tags](#tags) | `RECORD`| All instances affected by the event.  |

### City

| Column | Type | Description |
| :--- | :--- | :--- |
| id | `INTEGER`| Id used by logistics to identify the city of the vendor within a country. |
| name | `STRING`| Name of the city in English. |

### Message

| Column | Type | Description |
| :--- | :--- | :--- |
| id | `STRING`| The message id of the event. |
| value | `STRING`| Value corresponding to the message. |

### Tags

| delivery_provider | `ARRAY<STRING>`| Whether the food is delivered by a platform (e.g., Foodora), by the vendor itself, or in another way. |
| cuisines | `ARRAY<STRING>`| The cuisine(s) in which the vendor specialised. |
| is_halal | `BOOLEAN`| Whether the food prepared by the vendor is halal or not. |
| chains | `STRING` | N/A. |
| tags | `STRING` | N/A. |
| vertical_type | `STRING`| Describe the vertical type of a vendor. In other words, it describes types of goods that are delivered. |
| customer_types | `ARRAY<STRING>` | N/A. |
| characteristics | `ARRAY<STRING>` | N/A. |
