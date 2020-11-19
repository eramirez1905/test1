# lg_payments_deposit_rules

Payments deposit rules with each row representing one rule

Clustered by field `rdbms_id`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid is unique and represents a rule |
| id | `INTEGER` |  |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| country_code | `STRING` |  |
| region | `STRING` |  |
| name | `STRING` |  |
| description | `STRING` |  |
| cycle_number | `INTEGER` |  |
| gross_earning_percentage | `INTEGER` |  |
| status | `STRING` |  |
| amount_local | `FLOAT` |  |
| created_by_email | `STRING` |  |
| approved_by_email | `STRING` |  |
| is_active | `BOOLEAN` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| approved_at_utc | `TIMESTAMP` |  |
| created_date_utc | `DATE` |  |
| [applies_to](#appliesto) | `ARRAY<RECORD>` |  |

## applies_to

| Name | Type | Description |
| :--- | :--- | :---        |
| lg_contract_ids | `ARRAY<INTEGER>` |  |
| lg_city_ids | `ARRAY<INTEGER>` |  |
