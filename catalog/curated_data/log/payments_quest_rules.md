# Payments Quest Rules

| Column | Type | Description |
| :--- | :--- | :--- |
| country_code | `STRING` | A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2. |
| created_date | `DATE` | The date when the rule was created. |
| created_at | `TIMESTAMP` | The datetime when the rule was created. |
| updated_at | `TIMESTAMP` | The datetime when the rule was updated. |
| id | `INTEGER` | Identifier of the payment rule used by Rooster. |
| type | `STRING` | Object to which rule applies (Eg: `PER_DELIVERY`, `PER_HOUR`). |
| sub_type | `STRING` | Further specifications to the `type` the rule applies to. |
| active | `BOOLEAN` | Flag indicating if the rule is active or not. |
| name | `STRING` | The denomination of the rule. |
| created_by | `STRING` | Email of the user who created the rule. |
| status | `STRING` | Current status of the rule (EG: `APPROVED`, `PENDING`). |
| duration | `STRING` | The repetition period of the payment. |
| start_date | `TIMESTAMP` | The datetime when the rules starts. |
| end_date | `TIMESTAMP` | The datetime when the rules ends. |
| start_time | `TIME` | The time when the rule starts to be applied. |
| end_time | `TIME` | The time when the rule ends to be applied. |
| acceptance_rate | `INTEGER` | The minimum acceptance rate to which rule applies. |
| no_show_limit | `INTEGER` | The maximum no shows allowed by the rule to be applied. |
| is_pay_below_threshold | `BOOLEAN` |Indicates if the payment is for the total number of object to which rule applies according to  `type` column or only for those that exceed the threshold. |
| is_negative | `BOOLEAN` |It indicates if the quest rule is negative. |
| [applies_to](#applies-to) | `<ARRAY>RECORD` | Records pertaining categories to which rule applies. |
| [cost_factors](#cost-factors) | `<ARRAY>RECORD` | The goals to be achieved in order for the rule to apply. |

## Applies To

| Column | Type | Description |
| :--- | :--- | :--- |
| contract_types | `<ARRAY>STRING` | The contract types to which rule applies. |
| contract_ids | `<ARRAY>INTEGER` | The contract ids to which rule applies. |
| rider_ids | `<ARRAY>INTEGER` | The Rooster rider_ids to which the rule applies. |
| vehicle_types | `<ARRAY>STRING` | The vehicle types to which rule applies. |
| job_titles | `<ARRAY>STRING` | The job titles to which rule applies. |
| starting_point_ids | `<ARRAY>INTEGER` | The starting point ids to which rule applies. |
| days_of_week | `<ARRAY>STRING` | The days of the week to which rule applies. |
| city_ids | `<ARRAY>INTEGER` | The ids of the cities to which rule applies. |
| vertical_types | `<ARRAY>STRING` | The vertical types to which rule applies. |

## Cost Factors

| Column | Type | Description |
| :--- | :--- | :--- |
| type | `STRING` | Type to which payment is referring to. |
| created_at | `TIMESTAMP` | The timestamp when the entry was created. |
| updated_at | `TIMESTAMP` | The timestamp when the entry was updated. |
| created_by | `STRING` | The email of the user who created the entry. |
| threshold | `INTEGER` | Minimum amount of payment type units a rider has to reach, before he receives the bonus. |
| amount| `FLOAT`| Units from payment type, which result in a payment. |
