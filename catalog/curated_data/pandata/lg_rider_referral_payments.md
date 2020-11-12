# lg_rider_referral_payments

Rider referral payments

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
| lg_city_uuid | `STRING` |  |
| lg_city_id | `INTEGER` |  |
| lg_contract_uuid | `STRING` |  |
| lg_contract_id | `INTEGER` |  |
| referrer_lg_rider_uuid | `STRING` |  |
| referrer_lg_rider_id | `INTEGER` |  |
| referee_lg_rider_uuid | `STRING` |  |
| referee_lg_rider_id | `INTEGER` |  |
| lg_payment_cycle_uuid | `STRING` |  |
| lg_payment_cycle_id | `INTEGER` |  |
| region | `STRING` |  |
| country_code | `STRING` |  |
| city_name | `STRING` |  |
| rider_name | `STRING` |  |
| contract_type | `STRING` |  |
| vehicle_type | `STRING` |  |
| status | `STRING` |  |
| paid_local | `NUMERIC` |  |
| payment_type_unit_amount | `FLOAT` |  |
| timezone | `STRING` |  |
| payment_cycle_start_at_utc | `TIMESTAMP` |  |
| payment_cycle_end_at_utc | `TIMESTAMP` |  |
| paid_period_start_at_utc | `TIMESTAMP` |  |
| paid_period_end_at_utc | `TIMESTAMP` |  |
| created_at_utc | `TIMESTAMP` |  |
| created_date_utc | `DATE` |  |
