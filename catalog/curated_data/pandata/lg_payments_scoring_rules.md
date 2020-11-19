# lg_payments_scoring_rules

Payments scoring rules with each row representing one rule

Clustered by field `rdbms_id`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid is unique and represents a rule |
| id | `INTEGER` |  |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| lg_city_uuid | `STRING` |  |
| lg_city_id | `INTEGER` |  |
| country_code | `STRING` |  |
| created_by_email | `STRING` |  |
| status | `STRING` |  |
| type | `STRING` |  |
| name | `STRING` |  |
| payment_unit | `STRING` |  |
| payment_unit_type | `STRING` |  |
| is_active | `BOOLEAN` |  |
| start_at_utc | `TIMESTAMP` |  |
| end_at_utc | `TIMESTAMP` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| [applies_to](#appliesto) | `ARRAY<RECORD>` |  |
| [cost_factors](#costfactors) | `ARRAY<RECORD>` |  |

## applies_to

| Name | Type | Description |
| :--- | :--- | :---        |
| lg_contract_ids | `ARRAY<INTEGER>` |  |
| lg_rider_ids | `ARRAY<INTEGER>` |  |
| lg_starting_point_ids | `ARRAY<INTEGER>` |  |
| lg_city_uuid | `STRING` |  |
| lg_city_id | `INTEGER` |  |
| vertical_types | `ARRAY<STRING>` |  |
| vehicle_types | `ARRAY<STRING>` |  |
| contract_types | `ARRAY<STRING>` |  |
| job_titles | `ARRAY<STRING>` |  |

## cost_factors

| Name | Type | Description |
| :--- | :--- | :---        |
| created_by_email | `STRING` |  |
| minimum_threshold | `INTEGER` |  |
| type | `STRING` |  |
| amount | `FLOAT` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
