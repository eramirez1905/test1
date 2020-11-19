# lg_payments_quest_rules

Payments quest rules with each row representing one rule

Clustered by field `rdbms_id`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid is unique and represents a rule |
| id | `INTEGER` |  |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| country_code | `STRING` |  |
| created_by_email | `STRING` |  |
| status | `STRING` |  |
| type | `STRING` |  |
| sub_type | `STRING` |  |
| name | `STRING` |  |
| duration_iso8601 | `STRING` |  |
| is_active | `BOOLEAN` |  |
| is_pay_below_threshold | `BOOLEAN` |  |
| is_negative | `BOOLEAN` |  |
| minimum_acceptance_rate | `INTEGER` |  |
| no_show_limit | `INTEGER` |  |
| start_time | `TIME` |  |
| end_time | `TIME` |  |
| start_at_utc | `TIMESTAMP` |  |
| end_at_utc | `TIMESTAMP` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| created_date_utc | `DATE` |  |
| [applies_to](#appliesto) | `ARRAY<RECORD>` |  |

## applies_to

| Name | Type | Description |
| :--- | :--- | :---        |
| lg_contract_ids | `ARRAY<INTEGER>` |  |
| lg_rider_ids | `ARRAY<INTEGER>` |  |
| lg_starting_point_ids | `ARRAY<INTEGER>` |  |
| lg_city_ids | `ARRAY<INTEGER>` |  |
| vertical_types | `ARRAY<STRING>` |  |
| vehicle_types | `ARRAY<STRING>` |  |
| contract_types | `ARRAY<STRING>` |  |
| job_titles | `ARRAY<STRING>` |  |
| days_of_week | `ARRAY<STRING>` |  |
