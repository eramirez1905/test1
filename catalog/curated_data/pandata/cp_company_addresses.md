# cp_company_addresses

Table of company addresses from corporate squad

Clustered by fields `rdbms_id`, `cp_company_uuid`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid is unique and represents a company address |
| id | `INTEGER` |  |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| cp_company_uuid | `STRING` | Each uuid represents a company |
| cp_company_id | `INTEGER` |  |
| cp_user_uuid | `STRING` |  |
| cp_user_id | `INTEGER` |  |
| address | `STRING` |  |
| address_other | `STRING` |  |
| building | `STRING` |  |
| campus | `STRING` |  |
| delivery_instructions | `STRING` |  |
| city_name | `STRING` |  |
| district | `STRING` |  |
| entrance | `STRING` |  |
| floor | `STRING` |  |
| latitude | `FLOAT` |  |
| longitude | `FLOAT` |  |
| post_code | `STRING` |  |
| room | `STRING` |  |
| structure | `STRING` |  |
| type | `INTEGER` |  |
| state | `STRING` |  |
| is_state_active | `BOOLEAN` |  |
| is_state_deleted | `BOOLEAN` |  |
| is_state_suspended | `BOOLEAN` |  |
| is_flexible_address | `INTEGER` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modified_utc | `TIMESTAMP` |  |
