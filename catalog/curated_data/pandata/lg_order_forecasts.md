# lg_order_forecasts

Table of order forecasts from a year ago in logistics. Partitioned by created_date_utc, clustered by rdbms_id.

Partitioned by field `created_date_utc` with each
partition being a time interval of `DAY`

Clustered by field `rdbms_id`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid is unique and represents an order forecast in logistics |
| lg_zone_id | `INTEGER` |  |
| lg_zone_uuid | `STRING` |  |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| country_code | `STRING` |  |
| model_name | `STRING` |  |
| orders_expected | `FLOAT` |  |
| is_valid | `BOOLEAN` |  |
| is_most_recent | `BOOLEAN` |  |
| timezone | `STRING` |  |
| forecast_for_utc | `TIMESTAMP` |  |
| forecast_for_local | `TIMESTAMP` |  |
| created_at_utc | `TIMESTAMP` |  |
| created_date_utc | `DATE` |  |
| [starting_points](#startingpoints) | `ARRAY<RECORD>` |  |

## starting_points

| Name | Type | Description |
| :--- | :--- | :---        |
| id | `INTEGER` |  |
| uuid | `STRING` |  |
| riders_needed | `NUMERIC` |  |
| no_shows_expected | `NUMERIC` |  |
| demand_for_local | `TIMESTAMP` |  |
| demand_for_utc | `TIMESTAMP` |  |
