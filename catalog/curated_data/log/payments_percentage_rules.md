# Payments Percentage Rules

| Column | Type | Description |
| :--- | :--- | :--- |
| country_code | `STRING` | A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2. |
| created_date | `DATE` | The date when the rule was created. |
| created_at | `TIMESTAMP` | The datetime when the rule was created. |
| updated_at | `TIMESTAMP` | The datetime when the rule was updated. |
| name | `STRING` | The denomination of the rule. |
| start_date | `TIMESTAMP` | The datetime when the rules starts. |
| end_date | `TIMESTAMP` | The datetime when the rules ends. |
| id | `INTEGER` | Identifier of the payment rule used by Rooster. |
| active | `BOOLEAN` | Flag indicating if the rule is active or not. |
| percentage | `FLOAT` | The percentage paid by the rule. |
| apply_to | `STRING` | It explains if the rule applies to `GROSS` OR `NET` earnings. |
| status | `STRING` | Current status of the rule (EG: `APPROVED`, `PENDING`). |
| created_by | `STRING` | Email of the user who created the rule. |
| [applies_to](#applies-to) | `<ARRAY>RECORD` | Records pertaining categories to which rule applies. |

## Applies To

| Column | Type | Description |
| :--- | :--- | :--- |
| contract_types | `<ARRAY>STRING` | The contract types to which rule applies. |
| city_ids | `<ARRAY>INTEGER` | The city ids to which rule applies. |
| job_titles | `<ARRAY>STRING` | The job titles to which rule applies. |
