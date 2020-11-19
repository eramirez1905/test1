# sb_subscriptions

Table of subscriptions with each row representing one subscription and it's associated payment(s).

Partitioned by field `created_date_utc` with each
partition being a time interval of `DAY`

Clustered by fields `rdbms_id`, `customer_uuid`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid is unique and represents a subscription |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| id | `INTEGER` |  |
| global_entity_id | `STRING` |  |
| customer_uuid | `STRING` | Each uuid represents a customer |
| customer_id | `INTEGER` |  |
| code | `STRING` |  |
| plan_code | `STRING` |  |
| customer_code | `STRING` |  |
| status | `STRING` |  |
| payment_token | `STRING` |  |
| started_at_local | `TIMESTAMP` |  |
| ended_at_local | `TIMESTAMP` |  |
| last_renewal_at_local | `TIMESTAMP` |  |
| next_renewal_at_local | `TIMESTAMP` |  |
| created_at_local | `TIMESTAMP` |  |
| updated_at_local | `TIMESTAMP` |  |
| created_date_local | `DATE` |  |
| started_at_utc | `TIMESTAMP` |  |
| ended_at_utc | `TIMESTAMP` |  |
| last_renewal_at_utc | `TIMESTAMP` |  |
| next_renewal_at_utc | `TIMESTAMP` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| created_date_utc | `DATE` |  |
| dwh_last_modified_utc | `TIMESTAMP` |  |
| [payments](#payments) | `ARRAY<RECORD>` |  |

## payments

| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` |  |
| id | `INTEGER` |  |
| gateway_reference | `STRING` |  |
| http_status | `INTEGER` |  |
| internal_reference | `STRING` |  |
| total_amount_local | `NUMERIC` |  |
| vat_amount_local | `NUMERIC` |  |
| net_amount_local | `NUMERIC` |  |
| status | `STRING` |  |
| is_status_success | `BOOLEAN` |  |
| is_status_failed | `BOOLEAN` |  |
| is_status_pending | `BOOLEAN` |  |
| paid_at_local | `TIMESTAMP` |  |
| paid_at_utc | `TIMESTAMP` |  |
| subscription_start_at_local | `TIMESTAMP` |  |
| subscription_start_at_utc | `TIMESTAMP` |  |
| subscription_end_at_local | `TIMESTAMP` |  |
| subscription_end_at_utc | `TIMESTAMP` |  |
| created_at_local | `TIMESTAMP` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_local | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |
