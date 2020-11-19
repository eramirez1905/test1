# Porygon Events

### This table is deprecated and will be removed on 27-Nov-2020, so please use the new table [Delivery Areas Events](delivery_areas_events.md).

This table lists all events that have been entered and triggered (manually or automatically) in the tool `Porygon`. 
Every event is activated and deactivated several times, depending on the associated activation threshold.
Every activation and deactivation is listed in the `events` array, related to the `Porygon` event (event_id) they belong to
and act on. The transaction_id and end_transaction_id are then to be understood as the identifiers of these activating or 
deactivating events.

| Column | Type | Description |
| :--- | :--- | :--- |
| country_code | `STRING`| A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2. |
| city_id | `INTEGER`| The numeric identifier a the city in which the event takes place. |
| timezone | `STRING`| The time zone of the city or in the case of manually triggered events, the country in which the event takes place. |
| event_id | `INTEGER`| The numeric identifier of an event in `Porygon`. |
| platform | `STRING`| The name of the platform (e.g. Talabat) on which the order was placed. |
| created_at | `TIMESTAMP`| The time at which the event was created in `Porygon`. |
| created_date | `DATE`| The date at which the event was created in `Porygon`. |
| is_shape_in_sync | `BOOLEAN`| Flag whether an event polygon is kept in sync with the polygon of a zone as referenced by zone_id. |
| transactions | `ARRAY<RECORD>`| Information about the history of activation and deactivation, shape, type, and activation threshold of an event. |

### Transactions

| Column | Type | Description |
| :--- | :--- | :--- |
| transaction_id | `INTEGER`| The numeric identifier of the beginning of the activation or deactivation of an event. |
| end_transaction_id | `INTEGER`| The numeric identifier of the end of the activation or deactivation of an event) |
| start_at | `TIMESTAMP`| The time at which the activation or deactivation has started. |
| end_at | `TIMESTAMP`| The time at which the activation or deactivation has stopped. |
| duration | `INTEGER`| The duration of the activation or deactivation event, in seconds. |
| message | `STRING`| Event message in all languages. JSON format. |
| tags | `STRING`| All instances affected by the event (eg: delivery_type, chains). JSON format. |
| title | `STRING`| The title of the event. |
| action | `STRING`| The type of the event : closure, shrinkage, delay, lockdown. |
| is_active | `BOOLEAN`| If true, the event is active, if not, the event is inactive. |
| is_halal | `BOOLEAN`| If true, the event affects restaurants selling halal food. |
| value | `INTEGER`| In the case of shrinkages, the drive time defining the area of the polygon in which the restaurant delivers. |
| activation_threshold | `INTEGER`| The delay threshold above which an event is activated. |
| deactivation_threshold | `INTEGER`| The delay threshold under which an event is deactivated. |
| zone_id | `INTEGER`| The numeric identifier of the zone in which the event has taken place. If the event has been triggered manually, the zone is set to null. Every null value has been replaced with 0. |
| shape | `GEOGRAPHY`| The polygon's geographical coordinates. |
