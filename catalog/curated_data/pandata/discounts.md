# discounts

Table of discounts with each row representing one discount

Clustered by field `rdbms_id`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid is unique and represents a discount |
| id | `INTEGER` |  |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| vendor_uuid | `STRING` |  |
| vendor_id | `INTEGER` |  |
| condition_object_uuid | `STRING` |  |
| condition_object_id | `INTEGER` |  |
| title | `STRING` |  |
| description | `STRING` |  |
| discount_text | `STRING` |  |
| banner_title | `STRING` |  |
| file_path | `STRING` |  |
| platforms | `STRING` |  |
| daily_limit | `INTEGER` |  |
| total_limit | `INTEGER` |  |
| foodpanda_ratio | `FLOAT` |  |
| bogo_discount_unit_type | `STRING` |  |
| expedition_type | `STRING` |  |
| discount_type | `STRING` |  |
| condition_type | `STRING` |  |
| discount_mgmt_created_by_type | `STRING` |  |
| discount_mgmt_updated_by_type | `STRING` |  |
| is_active | `BOOLEAN` |  |
| is_deleted | `BOOLEAN` |  |
| is_pro_discount | `BOOLEAN` |  |
| is_new_customer_only | `BOOLEAN` |  |
| is_condition_multiple_vendors | `BOOLEAN` |  |
| is_condition_vendor | `BOOLEAN` |  |
| is_condition_menu_category | `BOOLEAN` |  |
| is_condition_product | `BOOLEAN` |  |
| is_condition_product_variation | `BOOLEAN` |  |
| is_condition_multiple_menu_categories | `BOOLEAN` |  |
| is_condition_menu | `BOOLEAN` |  |
| is_condition_chain | `BOOLEAN` |  |
| is_condition_none | `BOOLEAN` |  |
| maximum_discount_amount_local | `FLOAT` |  |
| minimum_order_value_local | `FLOAT` |  |
| amount_local | `NUMERIC` |  |
| start_time_local | `TIME` |  |
| end_time_local | `TIME` |  |
| start_date_local | `DATE` |  |
| end_date_local | `DATE` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |
