# vouchers

Table of vouchers with each row representing one voucher

Partitioned by field `created_date_utc` with each
partition being a time interval of `DAY`

Clustered by fields `rdbms_id`, `customer_uuid`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid is unique and represents a voucher |
| id | `INTEGER` |  |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| customer_uuid | `STRING` | Each uuid represents a customer |
| customer_id | `INTEGER` |  |
| user_uuid | `STRING` |  |
| user_id | `INTEGER` |  |
| payment_type_ids | `ARRAY<STRING>` |  |
| code | `STRING` |  |
| customer_code | `STRING` |  |
| quantity | `INTEGER` |  |
| daily_usage_limit | `INTEGER` |  |
| customer_quantity | `INTEGER` |  |
| counter | `INTEGER` |  |
| code_generation_strategy_type | `STRING` |  |
| type | `STRING` |  |
| product_category_voucher_unit_type | `STRING` |  |
| value | `FLOAT` |  |
| foodpanda_ratio | `FLOAT` |  |
| purpose | `STRING` |  |
| description | `STRING` |  |
| channel | `STRING` |  |
| voucher_not_valid_message | `STRING` |  |
| is_general_use | `BOOLEAN` |  |
| is_order_min_value_for_promotional_products | `BOOLEAN` |  |
| is_new_customer | `BOOLEAN` |  |
| is_new_customer_vertical | `BOOLEAN` |  |
| is_auto_apply_enabled | `BOOLEAN` |  |
| is_unlimited | `BOOLEAN` |  |
| is_deleted | `BOOLEAN` |  |
| is_type_third_party | `BOOLEAN` |  |
| is_type_cashback_amount | `BOOLEAN` |  |
| is_type_amount | `BOOLEAN` |  |
| is_type_free_gift | `BOOLEAN` |  |
| is_type_delivery_fee | `BOOLEAN` |  |
| is_type_percentage | `BOOLEAN` |  |
| is_type_conditional_bogo | `BOOLEAN` |  |
| is_type_product_category | `BOOLEAN` |  |
| is_type_joker_amount | `BOOLEAN` |  |
| is_type_cashback_percentage | `BOOLEAN` |  |
| minimum_order_value_local | `FLOAT` |  |
| maximum_order_value_local | `FLOAT` |  |
| maximum_discount_amount_local | `FLOAT` |  |
| platforms | `ARRAY<STRING>` |  |
| expedition_types | `ARRAY<STRING>` |  |
| vertical_types | `ARRAY<STRING>` |  |
| start_date_local | `DATE` |  |
| stop_date_local | `DATE` |  |
| start_at_local | `TIMESTAMP` |  |
| stop_at_local | `TIMESTAMP` |  |
| created_date_utc | `DATE` |  |
| created_at_local | `TIMESTAMP` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_local | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |
