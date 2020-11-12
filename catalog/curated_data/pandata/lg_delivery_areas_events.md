# lg_delivery_areas_events

Table of delivery area events in logistics. Partitioned by created_date_utc

Partitioned by field `created_date_utc` with each
partition being a time interval of `DAY`

Clustered by field `rdbms_id`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid is unique and represents a transaction that is part of an event. |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| lg_event_uuid | `STRING` |  |
| lg_event_id | `INTEGER` |  |
| lg_transaction_uuid | `STRING` |  |
| lg_transaction_id | `INTEGER` |  |
| lg_end_transaction_uuid | `STRING` |  |
| lg_end_transaction_id | `INTEGER` |  |
| lg_city_uuid | `STRING` |  |
| lg_city_id | `INTEGER` |  |
| lg_zone_uuid | `STRING` |  |
| lg_zone_id | `INTEGER` |  |
| country_code | `STRING` |  |
| is_active | `BOOLEAN` |  |
| is_shape_in_sync | `BOOLEAN` |  |
| operation_type | `STRING` |  |
| action_type | `STRING` |  |
| city_name | `STRING` |  |
| value | `INTEGER` |  |
| activation_threshold | `INTEGER` |  |
| title | `STRING` |  |
| shape_geo | `GEOGRAPHY` |  |
| duration_in_seconds | `INTEGER` |  |
| timezone | `STRING` |  |
| active_from_utc | `TIMESTAMP` |  |
| active_to_utc | `TIMESTAMP` |  |
| start_at_utc | `TIMESTAMP` |  |
| end_at_utc | `TIMESTAMP` |  |
| created_date_utc | `DATE` |  |
| [tags](#tags) | `RECORD` |  |
| [message](#message) | `ARRAY<RECORD>` |  |

## tags

| Name | Type | Description |
| :--- | :--- | :---        |
| delivery_provider | `ARRAY<STRING>` |  |
| cuisines | `ARRAY<STRING>` |  |
| is_halal | `BOOLEAN` |  |
| tag | `ARRAY<STRING>` |  |
| chains | `ARRAY<STRING>` |  |
| vertical_type | `STRING` |  |
| customer_types | `ARRAY<STRING>` |  |
| characteristics | `ARRAY<STRING>` |  |

## message

| Name | Type | Description |
| :--- | :--- | :---        |
| id | `STRING` |  |
| value | `STRING` |  |
