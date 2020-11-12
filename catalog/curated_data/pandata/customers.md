# customers

Table of customers with each row representing one customer nested with addresses

Clustered by field `rdbms_id`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid is unique and represents a customer |
| id | `INTEGER` |  |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| created_by_user_uuid | `STRING` |  |
| created_by_user_id | `INTEGER` |  |
| updated_by_user_uuid | `STRING` |  |
| updated_by_user_id | `INTEGER` |  |
| loyalty_program_name_uuid | `STRING` |  |
| loyalty_program_name_id | `INTEGER` |  |
| code | `STRING` |  |
| is_deleted | `BOOLEAN` |  |
| is_guest | `BOOLEAN` |  |
| is_mobile_verified | `BOOLEAN` |  |
| is_newsletter_user | `BOOLEAN` |  |
| email | `STRING` |  |
| first_name | `STRING` |  |
| last_name | `STRING` |  |
| internal_comment | `STRING` |  |
| landline | `STRING` |  |
| mobile | `STRING` |  |
| mobile_confirmation_code | `STRING` |  |
| mobile_country_code | `STRING` |  |
| mobile_verified_attempts | `INTEGER` |  |
| source | `STRING` |  |
| language_title | `STRING` |  |
| last_user_agent | `STRING` |  |
| last_login_at_local | `TIMESTAMP` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |
| [social_logins](#sociallogins) | `ARRAY<RECORD>` |  |

## social_logins

| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` |  |
| id | `INTEGER` |  |
| platform | `STRING` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |
