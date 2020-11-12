# Rider Compliance

This table aggregates all rider compliance violations and actions done. 

| Column | Type | Description |
| :--- | :--- | :--- |
| country_code | `STRING`| A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2. |
| created_date | `DATE`| Date of when the violation is supposed to be processed. **This date is in local time** |
| rider_id | `INTEGER`| Identifier of the paid rider used by Rooster. |
| rider_name | `STRING`| Rider's name and surname used by Rooster. |
| timezone | `STRING`| The name of the timezone where the city is located. The timezone enables time conversion, from UTC to local time. |
| [violations](#violations) | `ARRAY<RECORD>` | Array containing all the violations with their details. |

## Violations

| Column | Type | Description |
| :--- | :--- | :--- |
| id | `INTEGER`| Unique identifier of the violation within the country. |
| created_date | `DATE`| Date of when the violation is supposed to be processed. **This date is in local time** |
| city_name | `STRING`| Name of the city in English. **Will be deprecated on December 21st, 2020. Do not use**  |
| rider_id | `INTEGER`| Identifier of the paid rider used by Rooster. |
| created_at | `TIMESTAMP`| Timestamp indicating when the violation is initially created in the database. |
| process_at | `TIMESTAMP`| Timestamp indicating when the violation is planned to be processed. |
| processed_at | `TIMESTAMP`| Timestamp indicating when the violation was processed. |
| cancelled_at | `TIMESTAMP`| Timestamp indicating when the violation was cancelled. |
| state | `STRING`| The current state of the violation. This can be `SCHEDULED`, `PROCESSED` or `CANCELLED` and it related to the timestamp. |
| [actions](#actions) | `<ARRAY>RECORD`| The details pertaining the action performed for the violation. |
| [rules](#rules) | `<ARRAY>RECORD`| The details pertaining the rule related to the violation. |


## Actions

| Column | Type | Description |
| :--- | :--- | :--- |
| started_at | `TIMESTAMP`| Timestamp indicating when the action began. |
| ended_at | `TIMESTAMP`| Timestamp indicating when the action finished. |
| cancelled_at | `TIMESTAMP`| Timestamp indicating when the action was cancelled. |
| interrupted | `TIMESTAMP`| Timestamp indicating when the action was interrupted. (For example: rider brings cash back. |
| state | `STRING`| The state of the action. |
| type | `STRING`| The type of the action. (For example: `NOTIFICATION`, `SUSPENSION`, `TERMINATION`. |
| schedule | `STRING`| The contraint of the action. (For example: `AFTER_ACTIVE_SHIFT` means that the action can't be executed during active shift, 'INSTANTLY' means no contraint). |
| duration | `STRING`| The duration of the action. |
| name | `STRING`| The name assigned to that action. |

## Rules

| Column | Type | Description |
| :--- | :--- | :--- |
| id | `INTEGER`| Identifier of the rule in Rooster within the country. |
| city_id | `INTEGER`| The numeric identifier assigned to every city. This id is generated within the central logistics systems and is only unique within the same country. **Will be deprecated on December 21st, 2020. Do not use** |
| violation_type | `STRING`| The type of the violation to which the rule applies to. |
| rule_type | `STRING`| Simple Rule or Cumulative Rule. |
| violation_type | `STRING`| The type of the violation to which the rule applies to (Fake GPS, Cash Balance, Cancelled Orders). |
| contract_type | `STRING`| The contract type of the rider to which the rule applies to. |
| violation_duration | `STRING`| The duration for Simple Rule  (For example: if rule is "Cash balance over Hard limit within 1 hour" then violation_duration is 1 hour). |
| violation_count| `INTEGER`| How many violations need to be done for the rule to apply. |
| period_duration| `STRING`| The period of time for Cumulative Rule (For example: if rule is "3 times within 2 weeks" then period_duration is 2 weeks). |
