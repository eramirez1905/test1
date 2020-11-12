# Payments Scoring Rules

| Column | Type | Description |
| :--- | :--- | :--- |
| country_code | `STRING` | A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2. |
| created_date | `DATE` | The date when the rule was created. |
| created_at | `TIMESTAMP` | The datetime when the rule was created. |
| updated_at | `TIMESTAMP` | The datetime when the rule was updated. |
| id | `INTEGER` | Identifier of the payment rule used by Rooster. |
| type | `STRING` | The batch number to which rule applies. |
| name | `STRING` | The denomination of the rule. |
| paid_unit | `STRING` | Object to which rule applies (Eg: `PER_DELIVERY`, `PER_HOUR`) |
| paid_unit_type | `STRING` | Further specifications to the paid_unit |
| status | `STRING` | Current status of the rule (EG: `APPROVED`, `PENDING`). |
| active | `BOOLEAN` | Flag indicating if the rule is active or not. |
| created_by | `STRING` | Email of the user who created the rule. |
| city_id | `INTEGER` | The numeric identifier assigned to every city. This id is generated within the central logistics systems and is only unique within the same country. |
| start_date | `TIMESTAMP` | The datetime when the rules starts. |
| end_date | `TIMESTAMP` | The datetime when the rules ends. |
| [applies_to](#applies-to) | `<ARRAY>RECORD` | Records pertaining categories to which rule applies. |
| [cost_factors](#cost-factors) | `<ARRAY>RECORD` | The goals to be achieved in order for the rule to apply. |

## Applies To

| Column | Type | Description |
| :--- | :--- | :--- |
| contract_types | `<ARRAY>STRING` | The contract types to which rule applies. |
| contract_ids | `<ARRAY>INTEGER` | The identifier of the contracts to which rule applies. |
| rider_ids | `<ARRAY>INTEGER` | The Rooster rider_ids to which the rule applies. |
| vehicle_types | `<ARRAY>STRING` | The vehicle types to which rule applies. |
| job_titles | `<ARRAY>STRING` | The job titles to which rule applies. |
| starting_point_ids | `<ARRAY>INTEGER` | The starting point ids to which rule applies. |
| vertical_types | `<ARRAY>STRING` | The vertical types to which rule applies. |

## Cost Factors

| Column | Type | Description |
| :--- | :--- | :--- |
| type | `STRING` | Type to which payment is referring to. Batch for scoring). |
| created_at | `TIMESTAMP` | The timestamp when the entry was created. |
| updated_at | `TIMESTAMP` | The timestamp when the entry was updated. |
| created_by | `STRING` | The email of the user who created the entry. |
| threshold | `INTEGER` | Minimum amount of payment type units a rider has to reach, before he receives the bonus. |
| amount| `FLOAT`| Units from payment type, which result in a payment. |
