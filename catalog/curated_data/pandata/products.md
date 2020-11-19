# products

Table of products with each row representing one product

Clustered by field `rdbms_id`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` |  |
| id | `INTEGER` |  |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| menu_category_id | `INTEGER` |  |
| code | `STRING` |  |
| title | `STRING` |  |
| description | `STRING` |  |
| image_pathname | `STRING` |  |
| vat_rate | `STRING` |  |
| is_active | `BOOLEAN` |  |
| is_deleted | `BOOLEAN` |  |
| is_dish_information_excluded | `BOOLEAN` |  |
| has_dish_information | `BOOLEAN` |  |
| has_topping | `BOOLEAN` |  |
| is_express_item | `BOOLEAN` |  |
| is_prepacked_item | `BOOLEAN` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modifed_at_utc | `TIMESTAMP` |  |
| [variations](#variations) | `ARRAY<RECORD>` |  |

## variations

| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` |  |
| id | `INTEGER` |  |
| code | `STRING` |  |
| title | `STRING` |  |
| price_local | `FLOAT` |  |
| sponsorship_price_local | `FLOAT` |  |
| container_price_local | `NUMERIC` |  |
| is_deleted | `BOOLEAN` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modifed_at_utc | `TIMESTAMP` |  |
