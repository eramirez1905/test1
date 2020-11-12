# lg_payments_referral_rules

Payments referral rules with each row representing one rule

Clustered by field `rdbms_id`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid is unique and represents a rule |
| id | `INTEGER` |  |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| country_code | `STRING` |  |
| threshold | `FLOAT` |  |
| status | `STRING` |  |
| applies_to_type | `STRING` |  |
| payment_unit_type | `STRING` |  |
| payment_unit_amount | `NUMERIC` |  |
| duration_iso8601 | `STRING` |  |
| created_by_email | `STRING` |  |
| is_active | `BOOLEAN` |  |
| has_signing_bonus | `BOOLEAN` |  |
| is_status_approved | `BOOLEAN` |  |
| is_status_rejected | `BOOLEAN` |  |
| is_status_pending | `BOOLEAN` |  |
| signing_bonus_local | `NUMERIC` |  |
| start_at_utc | `TIMESTAMP` |  |
| end_at_utc | `TIMESTAMP` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| created_date_utc | `DATE` |  |
