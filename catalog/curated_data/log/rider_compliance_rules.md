## Rider Compliance Rules

| Column | Type | Description |
| :--- | :--- | :--- |
| region | `STRING`| The two-letter code of the operational region in which the country is located. The three operational regions are America (US), Europe and Middle East (EU), Asia (AP). |
| country_code | `STRING`| A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2. |
| created_date | `DATE`| Date when the rule is created in the application. |
| rule_id | `INTEGER`| The identifier of the rule. |
| rule_name | `STRING`| The denomination of the rule. |
| violation_type | `STRING`|The type of the violation to which the rule applies to (Fake GPS, Cash Balance, Cancelled Orders). |
| contract_type | `STRING`| The contract type of the rider to which the rule applies to. |
| created_at | `TIMESTAMP`| Timestamp indicating when the rule was created. |
| city_id | `INTEGER`| The numeric identifier assigned to every city. This id is generated within the central logistics systems and is only unique within the same country. **Will be deprecated on December 21st, 2020. Use `cities` array instead** |
| city_name | `STRING`| Name of the city in English. **Will be deprecated on December 21st, 2020. Use `cities` array instead**  |
| timezone | `STRING`| The name of the timezone of the country. The timezone enables time conversion, from UTC to local time. |
| active| `BOOLEAN`| How many violations need to be done for the rule to apply. |
| violation_duration| `STRING`| The duration for Simple Rule  (For example: if rule is "Cash balance over Hard limit within 1 hour" then violation_duration is 1 hour). |
| violation_category| `STRING`| . |
| rule_type| `STRING`| . |
| violation_count| `INTEGER`| How many violations need to be done for the rule to apply. |
| period_duration| `STRING`| . |
| [applies_to](#applies-to) | `<ARRAY>RECORD` | Records pertaining categories to which rule applies. |
| [actions_template](#actions-template) | `<ARRAY>RECORD` | Records pertaining the actions that will be done by the rule. |

## Applies To

| Column | Type | Description |
| :--- | :--- | :--- |
| rider_ids | `<ARRAY>INTEGER` | The Rooster rider_ids to which the rule applies. |
| batch_numbers | `<ARRAY>INTEGER` | The batch numbers to which rule applies. |
| [cities](#cities) | `<ARRAY>RECORD` | The cities information to which rule applies. |

## Cities

| Column | Type | Description |
| :--- | :--- | :--- |
| id | `INTEGER`| The numeric identifier assigned to every city. This id is generated within the central logistics systems and is only unique within the same country. |
| name | `STRING`| Name of the city in English. |
| timezone | `STRING`| The name of the timezone where the city is located. The timezone enables time conversion, from UTC to local time. |

## Actions Template

| Column | Type | Description |
| :--- | :--- | :--- |
| type | `STRING` | The type of the action. (For example: `NOTIFICATION`, `SUSPENSION`, `TERMINATION`). |
| duration | `STRING` | The duration of the action. |
| schedule | `STRING` | The contraint of the action. (For example: `AFTER_ACTIVE_SHIFT` means that the action can't be executed during active shift, 'INSTANTLY' means no contraint). |
| notification_text | `STRING` | The content of the push notification sent to rider. |
| name | `STRING` | The name of the template. |
| id | `INTEGER` | The identifier of template. |
