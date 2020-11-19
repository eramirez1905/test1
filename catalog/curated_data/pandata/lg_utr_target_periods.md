# lg_utr_target_periods

UTR by periods with Historical changes of UTR suggestions

Clustered by field `rdbms_id`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid is unique and represents a UTR target period |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| lg_city_uuid | `STRING` |  |
| lg_city_id | `INTEGER` |  |
| lg_zone_uuid | `STRING` |  |
| lg_zone_id | `INTEGER` |  |
| country_code | `STRING` |  |
| period_name | `STRING` |  |
| weekday | `STRING` |  |
| start_time_local | `TIME` |  |
| end_time_local | `TIME` |  |
| timezone | `STRING` |  |
| created_at_utc | `TIMESTAMP` |  |
| [history](#history) | `ARRAY<RECORD>` |  |

## history

| Name | Type | Description |
| :--- | :--- | :---        |
| updated_at_utc | `TIMESTAMP` |  |
| suggested_utr | `FLOAT` |  |
| utr | `FLOAT` |  |
| is_latest | `BOOLEAN` |  |
