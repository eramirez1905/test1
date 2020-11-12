# customer_addresses

Table of customer addresses with each row representing one customer address

Clustered by fields `rdbms_id`, `customer_uuid`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid is unique and represents a customer address |
| id | `INTEGER` |  |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| area_uuid | `STRING` |  |
| area_id | `INTEGER` |  |
| city_uuid | `STRING` |  |
| city_id | `INTEGER` |  |
| corporate_reference_uuid | `STRING` |  |
| corporate_reference_id | `INTEGER` |  |
| customer_uuid | `STRING` | This address belongs to the customer with this uuid. A customer can have more than one address. |
| customer_id | `INTEGER` |  |
| created_by_user_uuid | `STRING` |  |
| created_by_user_id | `INTEGER` |  |
| updated_by_user_uuid | `STRING` |  |
| updated_by_user_id | `INTEGER` |  |
| is_deleted | `BOOLEAN` |  |
| is_default | `BOOLEAN` |  |
| is_vendor_custom_location | `BOOLEAN` |  |
| is_verified | `BOOLEAN` |  |
| title | `STRING` |  |
| type | `INTEGER` |  |
| address_type | `STRING` |  |
| address | `STRING` |  |
| address_other | `STRING` |  |
| building | `STRING` |  |
| campus | `STRING` |  |
| city | `STRING` |  |
| district | `STRING` |  |
| company | `STRING` |  |
| customer_code | `STRING` |  |
| delivery_instructions | `STRING` |  |
| entrance | `STRING` |  |
| flat_number | `STRING` |  |
| floor | `STRING` |  |
| formatted_address | `STRING` |  |
| intercom | `STRING` |  |
| postcode | `STRING` |  |
| room | `STRING` |  |
| structure | `STRING` |  |
| position | `INTEGER` |  |
| latitude | `NUMERIC` |  |
| longitude | `NUMERIC` |  |
| last_usage_at_local | `TIMESTAMP` |  |
| created_at_local | `TIMESTAMP` |  |
| updated_at_local | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |
