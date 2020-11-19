# lg_rider_wallet_transactions

Rider wallet transactions in logistics

Partitioned by field `created_date_utc` with each
partition being a time interval of `DAY`

Clustered by fields `rdbms_id`, `lg_rider_uuid`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid represents a rider wallet transaction |
| id | `STRING` |  |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| lg_rider_uuid | `STRING` | Each uuid represents a rider in logistics |
| lg_rider_id | `INTEGER` |  |
| lg_delivery_uuid | `STRING` |  |
| lg_delivery_id | `INTEGER` |  |
| lg_order_uuid | `STRING` |  |
| lg_order_id | `INTEGER` |  |
| lg_city_uuid | `STRING` |  |
| lg_city_id | `INTEGER` |  |
| external_provider_id | `STRING` |  |
| integrator_id | `STRING` |  |
| country_code | `STRING` |  |
| type | `STRING` |  |
| amount_local | `INTEGER` |  |
| balance_local | `INTEGER` |  |
| created_by | `STRING` |  |
| note | `STRING` |  |
| is_manual | `BOOLEAN` |  |
| is_mass_update | `BOOLEAN` |  |
| timezone | `STRING` |  |
| created_date_utc | `DATE` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| [issues](#issues) | `ARRAY<RECORD>` |  |

## issues

| Name | Type | Description |
| :--- | :--- | :---        |
| id | `STRING` |  |
| original_amount_local | `INTEGER` |  |
| justification | `STRING` |  |
| is_reviewed | `BOOLEAN` |  |
| is_approved | `BOOLEAN` |  |
| created_by | `STRING` |  |
| updated_by | `STRING` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
