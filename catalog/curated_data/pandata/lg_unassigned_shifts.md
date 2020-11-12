# lg_unassigned_shifts

Unassigned shifts in logistics

Partitioned by field `created_date_utc` with each
partition being a time interval of `DAY`

Clustered by field `rdbms_id`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid is unique and represents an unassigned shift |
| id | `INTEGER` |  |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| lg_city_uuid | `STRING` |  |
| lg_city_id | `INTEGER` |  |
| lg_zone_uuid | `STRING` |  |
| lg_zone_id | `INTEGER` |  |
| lg_starting_point_uuid | `STRING` |  |
| lg_starting_point_id | `INTEGER` |  |
| country_code | `STRING` |  |
| created_by_lg_user_id | `INTEGER` |  |
| updated_by_lg_user_id | `INTEGER` |  |
| is_created_automatically | `BOOLEAN` |  |
| is_updated_automatically | `BOOLEAN` |  |
| is_state_published | `BOOLEAN` |  |
| is_state_pending | `BOOLEAN` |  |
| is_state_cancelled | `BOOLEAN` |  |
| is_tag_automatic | `BOOLEAN` |  |
| is_tag_copied | `BOOLEAN` |  |
| is_tag_termination | `BOOLEAN` |  |
| is_tag_starting_point_unassigned | `BOOLEAN` |  |
| is_tag_manual | `BOOLEAN` |  |
| is_tag_swap | `BOOLEAN` |  |
| slots | `INTEGER` |  |
| tag | `STRING` |  |
| state | `STRING` |  |
| timezone | `STRING` |  |
| start_at_local | `TIMESTAMP` |  |
| start_at_utc | `TIMESTAMP` |  |
| end_at_local | `TIMESTAMP` |  |
| end_at_utc | `TIMESTAMP` |  |
| created_at_local | `TIMESTAMP` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_local | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| created_date_utc | `DATE` |  |
