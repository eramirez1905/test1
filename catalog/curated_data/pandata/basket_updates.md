# basket_updates

Table of basket updates with each row representing one basket update

Partitioned by field `created_date_utc` with each
partition being a time interval of `DAY`

Clustered by field `rdbms_id`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid is unique and represents a basket update |
| id | `INTEGER` |  |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| meta_source | `STRING` |  |
| meta_user_type | `STRING` |  |
| order_id | `INTEGER` |  |
| created_at_utc | `TIMESTAMP` |  |
| created_date_utc | `DATE` |  |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |
| [products](#products) | `ARRAY<RECORD>` |  |

## products

| Name | Type | Description |
| :--- | :--- | :---        |
| product_uuid | `STRING` |  |
| product_id | `INTEGER` |  |
| product_variation_uuid | `STRING` |  |
| product_variation_id | `INTEGER` |  |
| action | `STRING` |  |
| original_quantity | `INTEGER` |  |
| reason | `STRING` |  |
| title | `STRING` |  |
| variation_title | `STRING` |  |
| packaging_fee_gross_local | `FLOAT` |  |
| subtotal_gross_local | `FLOAT` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |
| [toppings](#toppings) | `ARRAY<RECORD>` |  |

## toppings

| Name | Type | Description |
| :--- | :--- | :---        |
| topping_template_product_uuid | `STRING` |  |
| topping_template_product_id | `INTEGER` |  |
| title | `STRING` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |
