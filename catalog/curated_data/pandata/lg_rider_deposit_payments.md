# lg_rider_deposit_payments

Rider deposit payments in logistics

Partitioned by field `created_date_utc` with each
partition being a time interval of `DAY`

Clustered by fields `rdbms_id`, `lg_rider_uuid`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid represents a rider deposit payment |
| id | `INTEGER` |  |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| country_code | `STRING` |  |
| lg_rider_uuid | `STRING` | Each uuid represents a rider in logistics |
| lg_rider_id | `INTEGER` |  |
| lg_payment_cycle_uuid | `STRING` |  |
| lg_payment_cycle_id | `INTEGER` |  |
| lg_payments_deposit_rule_uuid | `STRING` |  |
| lg_payments_deposit_rule_id | `INTEGER` |  |
| status | `STRING` |  |
| paid_amount_local | `FLOAT` |  |
| remaining_amount_local | `FLOAT` |  |
| rider_name | `STRING` |  |
| region | `STRING` |  |
| timezone | `STRING` |  |
| created_date_utc | `DATE` |  |
| payment_cycle_start_at_utc | `TIMESTAMP` |  |
| payment_cycle_end_at_utc | `TIMESTAMP` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
