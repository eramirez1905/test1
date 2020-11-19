# Payments Referral Rules

| Column | Type | Description |
| :--- | :--- | :--- |
| country_code | `STRING` | A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2. |
| created_date | `DATE` | The date when the rule was created. |
| created_at | `TIMESTAMP` | The datetime when the rule was created. |
| updated_at | `TIMESTAMP` | The datetime when the rule was updated. |
| start_date | `TIMESTAMP` | The datetime when the rules starts. |
| end_date | `TIMESTAMP` | The datetime when the rules ends. |
| id | `INTEGER` | Identifier of the payment rule used by Rooster. |
| unit_type | `STRING` | Object to which rule applies (Eg: `PER_DELIVERY`, `PER_HOUR`). |
| threshold | `FLOAT` | The minimum amount of units the referee should reach for the rule to apply. |
| active | `BOOLEAN` | Flag indicating if the rule is active or not. |
| signing_bonus | `BOOLEAN` | If rules defines a bonus upon creation of the referee. |
| signing_bonus_amount | `NUMERIC` | The amount of the payment if there is a signing bonus. |
| amount | `FLOAT` | The amount of the payment rule. |
| status | `STRING` | Current status of the rule (EG: `APPROVED`, `PENDING`). |
| created_by | `STRING` | Email of the user who created the rule. |
| duration | `STRING` | The repetition period of the payment. |
| applies_to | `STRING` | Categories to which rule applies. (EG: `ALL`, `REFEREE`, `REFERRER`) |
