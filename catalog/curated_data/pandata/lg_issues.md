# lg_issues

Table of issues in logistics

Partitioned by field `created_date_utc` with each
partition being a time interval of `DAY`

Clustered by field `rdbms_id`


| Name | Type | Description |
| :--- | :--- | :---        |
| id | `STRING` |  |
| uuid | `STRING` | Each id is unique and represents an issue |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| lg_delivery_id | `INTEGER` |  |
| lg_delivery_uuid | `STRING` |  |
| lg_rider_id | `INTEGER` |  |
| lg_rider_uuid | `STRING` |  |
| lg_city_id | `INTEGER` |  |
| lg_city_uuid | `STRING` |  |
| lg_zone_id | `INTEGER` |  |
| lg_zone_uuid | `STRING` |  |
| country_code | `STRING` |  |
| name | `STRING` |  |
| notes | `STRING` |  |
| is_shown_on_watchlist | `BOOLEAN` |  |
| type | `STRING` |  |
| category | `STRING` |  |
| timezone | `STRING` |  |
| picked_at_utc | `TIMESTAMP` |  |
| picked_by_utc | `STRING` |  |
| dismissed_at_utc | `TIMESTAMP` |  |
| resolved_at_utc | `TIMESTAMP` |  |
| created_date_utc | `DATE` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| [actions](#actions) | `ARRAY<RECORD>` |  |

## actions

| Name | Type | Description |
| :--- | :--- | :---        |
| id | `STRING` |  |
| name | `STRING` |  |
| status | `STRING` |  |
| time_to_trigger_in_minutes | `INTEGER` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| [conditions](#conditions) | `ARRAY<RECORD>` |  |

## conditions

| Name | Type | Description |
| :--- | :--- | :---        |
| type | `STRING` |  |
| value | `STRING` |  |
