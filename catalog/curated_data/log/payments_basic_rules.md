# Payments Basic Rules

| Column | Type | Description |
| :--- | :--- | :--- |
| country_code | `STRING` | A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2. |
| created_date | `DATE` | The date when the rule was created. |
| created_at | `TIMESTAMP` | The datetime when the rule was created. |
| updated_at | `TIMESTAMP` | The datetime when the rule was updated. |
| id | `INTEGER` | Identifier of the payment rule used by Rooster. |
| type | `STRING` | Object to which rule applies (Eg: `PER_DELIVERY`, `PER_HOUR`). |
| sub_type | `STRING` | Further specifications to the `type` the rule applies to. |
| name | `STRING` | The denomination of the rule. |
| amount | `FLOAT` | The amount of payment. |
| created_by | `STRING` | Email of the user who created the rule. |
| status | `STRING` | Current status of the rule (EG: `APPROVED`, `PENDING`). |
| active | `BOOLEAN` | Flag indicating if the rule is active or not. |
| start_date | `TIMESTAMP` | The datetime when the rules starts. |
| end_date | `TIMESTAMP` | The datetime when the rules ends. |
| start_time | `TIME` | The time when the rule starts to be applied. |
| end_time | `TIME` | The time when the rule ends to be applied. |
| acceptance_rate | `INTEGER` | The minimum acceptance rate to which rule applies. |
| threshold | `FLOAT` | The min distance per delivery (can be either PU, DO, or both) from where we start paying the distance. |
| is_pay_below_threshold | `BOOLEAN` | Indicates if the payment is for the total distance or only for the distance above the threshold. |
| max_threshold | `FLOAT` | The max distance that is paid in a delivery. |
| [applies_to](#applies-to) | `<ARRAY>RECORD` | Records pertaining categories to which rule applies. |

## Applies To

| Column | Type | Description |
| :--- | :--- | :--- |
| contract_types | `<ARRAY>STRING` | The contract types to which rule applies. |
| contract_ids | `<ARRAY>INTEGER` | The contract ids to which rule applies. |
| rider_ids | `<ARRAY>INTEGER` | The Rooster rider_ids to which the rule applies. |
| vehicle_types | `<ARRAY>STRING` |  The vehicle types to which rule applies. |
| job_titles | `<ARRAY>STRING` | The job titles to which rule applies. |
| starting_point_ids | `<ARRAY>INTEGER` | The starting point ids to which rule applies. |
| days_of_week | `<ARRAY>STRING` | The days of the week to which rule applies. |
| city_ids | `<ARRAY>INTEGER` | The ids of the cities to which rule applies. |
| vertical_types | `<ARRAY>STRING` | The vertical types to which rule applies. |

