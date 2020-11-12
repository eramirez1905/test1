# users

Table of users with each row representing one user. A user is a user on pandora's backend, and is different from a customer that purchases from the platform.

Clustered by field `rdbms_id`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid is unique and represents a user |
| id | `INTEGER` |  |
| rdbms_id | `INTEGER` |  |
| created_by_user_id | `INTEGER` |  |
| updated_by_user_id | `INTEGER` |  |
| email | `STRING` |  |
| first_name | `STRING` |  |
| last_name | `STRING` |  |
| phone | `STRING` |  |
| username | `STRING` |  |
| is_active | `BOOLEAN` |  |
| is_deleted | `BOOLEAN` |  |
| is_global | `BOOLEAN` |  |
| is_visible | `BOOLEAN` |  |
| is_automated | `BOOLEAN` |  |
| last_login_at_local | `TIMESTAMP` |  |
| created_at_local | `TIMESTAMP` |  |
| updated_at_local | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |
| [roles](#roles) | `ARRAY<RECORD>` |  |

## roles

| Name | Type | Description |
| :--- | :--- | :---        |
| title | `STRING` |  |
| landing_page | `STRING` |  |
| priority | `INTEGER` |  |
| order_list_headers | `STRING` |  |
| has_audio_alert | `BOOLEAN` |  |
| has_modal_alert | `BOOLEAN` |  |
| has_vendor_role | `BOOLEAN` |  |
| is_direct_order_assignment_allowed | `BOOLEAN` |  |
| is_visible | `BOOLEAN` |  |
| code_type | `STRING` |  |
| created_at_local | `TIMESTAMP` |  |
| updated_at_local | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |
