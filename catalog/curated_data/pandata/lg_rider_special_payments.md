# lg_rider_special_payments

Rider special payments in logistics

Partitioned by field `created_date_utc` with each
partition being a time interval of `DAY`

Clustered by fields `rdbms_id`, `lg_rider_uuid`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid represents a referral payment |
| id | `INTEGER` |  |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| lg_rider_uuid | `STRING` | Each uuid represents a rider in logistics |
| lg_rider_id | `INTEGER` |  |
| lg_payment_cycle_uuid | `STRING` |  |
| lg_payment_cycle_id | `INTEGER` |  |
| region | `STRING` |  |
| country_code | `STRING` |  |
| rider_name | `STRING` |  |
| created_by_email | `STRING` |  |
| status | `STRING` |  |
| reason | `STRING` |  |
| note | `STRING` |  |
| paid_local | `NUMERIC` |  |
| adjustment_type | `STRING` |  |
| timezone | `STRING` |  |
| payment_cycle_start_at_utc | `TIMESTAMP` |  |
| payment_cycle_end_at_utc | `TIMESTAMP` |  |
| created_at_utc | `TIMESTAMP` |  |
| created_date_utc | `DATE` |  |
