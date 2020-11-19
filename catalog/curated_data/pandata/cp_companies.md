# cp_companies

Table of companies from corporate squad

Clustered by field `rdbms_id`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid is unique and represents a company |
| id | `INTEGER` |  |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| city | `STRING` |  |
| name | `STRING` |  |
| industry | `STRING` |  |
| postal_code | `STRING` |  |
| purpose | `STRING` |  |
| setting | `STRING` |  |
| state | `STRING` |  |
| street | `STRING` |  |
| is_state_active | `BOOLEAN` |  |
| is_state_deleted | `BOOLEAN` |  |
| is_state_new | `BOOLEAN` |  |
| is_state_suspended | `BOOLEAN` |  |
| is_allowance_enabled | `BOOLEAN` |  |
| is_requested_demo | `BOOLEAN` |  |
| is_self_signup | `BOOLEAN` |  |
| is_account_linking_required | `BOOLEAN` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modified_utc | `TIMESTAMP` |  |
| [departments](#departments) | `ARRAY<RECORD>` |  |

## departments

| Name | Type | Description |
| :--- | :--- | :---        |
| name | `STRING` |  |
| state | `STRING` |  |
| is_state_active | `BOOLEAN` |  |
| is_state_deleted | `BOOLEAN` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
