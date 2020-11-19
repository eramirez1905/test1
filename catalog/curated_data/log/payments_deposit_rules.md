# Payments Deposit Rules

| Column | Type | Description |
| :--- | :--- | :--- |
| region | `STRING`| The two-letter code of the operational region in which the country is located. The three operational regions are America (US), Europe and Middle East (EU), Asia (AP). |
| country_code | `STRING` | A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2. |
| created_date | `DATE` | The date when the rule was created. |
| id | `INTEGER` | Identifier of the payment rule used by Rooster. |
| created_by | `STRING` | Email of the user who created the rule. |
| created_at | `TIMESTAMP` | The datetime when the rule was created. |
| approved_at | `TIMESTAMP` | The datetime when the rule was approved. |
| updated_at | `TIMESTAMP` | The datetime when the rule was updated. |
| active | `BOOLEAN` | Flag indicating if the rule is active or not. |
| name | `STRING` | The denomination of the rule. |
| description | `STRING` | Further specifications on the rule. |
| amount | `FLOAT` | Amount to be paid according to the deposit rule. |
| cycle_number | `INTEGER` | Number of installments to pay. |
| gross_earning_percentage | `INTEGER` | Maximum percentage that can be deducted based on the gross earnings of the rider in the payment cycle. |
| status | `STRING` | Current status of the rule (EG: `APPROVED`, `PENDING`). |
| [applies_to](#applies-to) | `<ARRAY>RECORD` | Records pertaining categories to which rule applies. |

## Applies To

| Column | Type | Description |
| :--- | :--- | :--- |
| contract_ids | `<ARRAY>INTEGER` | The contract ids to which rule applies. |
| city_ids | `<ARRAY>INTEGER` | The ids of the cities to which rule applies. |
