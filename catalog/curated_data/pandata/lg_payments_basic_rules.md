# lg_payments_basic_rules

Payments basic rules with each row representing one rule

Clustered by field `rdbms_id`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid is unique and represents a rule |
| id | `INTEGER` |  |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| country_code | `STRING` |  |
| type | `STRING` |  |
| sub_type | `STRING` |  |
| name | `STRING` |  |
| amount_local | `FLOAT` |  |
| min_threshold_in_km | `FLOAT` |  |
| max_threshold_in_km | `FLOAT` |  |
| created_by_email | `STRING` |  |
| is_approved | `BOOLEAN` |  |
| status | `STRING` |  |
| is_active | `BOOLEAN` |  |
| is_pay_below_threshold | `BOOLEAN` |  |
| acceptance_rate | `INTEGER` |  |
| start_time | `TIME` |  |
| end_time | `TIME` |  |
| start_date_utc | `TIMESTAMP` |  |
| end_date_utc | `TIMESTAMP` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| [applies_to](#appliesto) | `ARRAY<RECORD>` |  |

## applies_to

| Name | Type | Description |
| :--- | :--- | :---        |
| lg_rider_ids | `ARRAY<INTEGER>` |  |
| lg_starting_point_ids | `ARRAY<INTEGER>` |  |
| lg_city_ids | `ARRAY<INTEGER>` |  |
| lg_contract_ids | `ARRAY<INTEGER>` |  |
| days_of_week | `ARRAY<STRING>` |  |
| contract_types | `ARRAY<STRING>` |  |
| vehicle_types | `ARRAY<STRING>` |  |
| job_titles | `ARRAY<STRING>` |  |
