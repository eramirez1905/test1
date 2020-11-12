# Riders


| Column | Type | Description |
| :--- | :--- | :--- |
| country_code | `STRING`| A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2. |
| rider_id | `INTEGER`| Identifier of the rider used by Rooster |
| rider_name | `STRING`| Rider's name and surname used by Rooster |
| email | `STRING`| Rider's email used by Rooster |
| phone_number | `STRING`| Rider's phone number used by Rooster |
| reporting_to | `INTEGER`| Rider_id of the rider to whom rider is reporting to |
| batch_number | `INTEGER`| Group assigned by biweekly scoring run in Rooster (current status) |
| created_at | `TIMESTAMP` | The date and time when the record has been created in UTC. |
| birth_date | `DATE` | The date of birth of the rider. **Use this field instead of `birth_date` in `custom_fields` as information is now stored under this field** |
| updated_at | `TIMESTAMP` | The date and time when rooster employee,contract or absence was updated. Can be used for Change data capture. |
| custom_fields | `ARRAY<RECORD>`| Custom fields in rooster providing additional information about the rider, for example {"address", "nationality", "birth_date", "referral_short_url"} |
| [absences history](#absences-history) | `ARRAY<RECORD>`| Information about all the absences of a specific rider. |
| [contracts](#contracts) | `ARRAY<RECORD>`| Information about the current and past contracts of a specific rider. |
| [batches](#batches) | `ARRAY<RECORD>`| Information about the past batch numbers of a specific rider. |
| [rider_vehicles](#rider-vehicles) | `ARRAY<RECORD>`| Information about the rider vehicle. |
| [starting_points](#starting-points) | `ARRAY<RECORD>`| Information about the starting points. |
| [rider_feedbacks](#rider-feedbacks) | `ARRAY<RECORD>` | Records pertaining to rider feedbacks given by Hurrier users.  |
| [nps](#nps) | `ARRAY<RECORD>` | Information about NPS for new and existing riders.  |

### Custom Fields

| Column | Type | Description |
| :--- | :--- | :--- |
| name | `STRING`| Name of the specific custom field, for example "birth_date") |
| type | `STRING`| data type of the value |
| value | `STRING`| Value of the specifiv custom field, for example "1990-01-01" |

### Absences History

| Column | Type | Description |
| :--- | :--- | :--- |
| id | `INTEGER`| The unique identifier of the absence used by Rooster. |
| start_at | `TIMESTAMP`| The start datetime of the absence. |
| end_at | `TIMESTAMP`| The end datetime of the absence. |
| reason | `STRING`| The explanation of why an absence is created.(Eg: Sickness). |
| is_paid | `BOOLEAN`| Flag indicating if the absence is paid or not. |
| status | `STRING`| Current status of the absence.(Eg: Accepted, Pending, Rejected). |
| user_id | `INTEGER`| The identifier of the user who uploaded the absence into Rooster. |
| created_at | `TIMESTAMP`| The datetime when the absence was created in Rooster. |
| updated_at | `TIMESTAMP`| The datetime when the absence was updated in Rooster. |
| comment | `STRING`| Any additional comment on the reason of the absence. |
| violation_id | `INTEGER`| The identifier of the compliance violation triggering the absence. |

### Contracts

| Column | Type | Description |
| :--- | :--- | :--- |
| id | `INTEGER`| Identifier of the contract used by Rooster |
| type | `STRING`| Contract type used by Rooster, for example {"PART_TIME", "FULL_TIME", "FREELANCER"}   |
| status | `STRING`| Possible values "VALID", "INVALID" |
| city_id | `INTEGER`| The numeric identifier assigned to every city |
| name | `STRING`| Name of the contract in Rooster |
| job_title | `STRING`| Rider's job title as assigned in Rooster |
| start_at | `STRING`| The date and time the contract started in UTC |
| end_at | `STRING`| The date and time the contract ended in UTC |
| termination_reason | `STRING`| Reason why contract was ended as selected in Rooster |
| termination_type | `STRING`| Values can be either "VOLUNTARY" or "NON_VOLUNTARY" |
| [interval_rules](#contract-interval-rules) | 'ARRAY<RECORD>' | N/A | 
| created_at | `TIMESTAMP`| The date and time the contract was created in Rooster in UTC |
| updated_at | `TIMESTAMP`| The date and time the contract was updated in Rooster in UTC |

### Contract Interval Rules 

| Column | Type | Description |
| :--- | :--- | :--- |
| id | `INTEGER`| Identifier of the contract interval rule used by Rooster |
| interval_rule_type | `STRING`| A limited set of pre-defined rules to select from: MIN_WORKLOAD_IN_PERIOD, <br> MAX_WORKLOAD_IN_PERIOD, <br> MIN_BREAK, <br> MAX_BREAK, <br> MIN_SHIFT_LENGTH, <br> MAX_SHIFT_LENGTH, <br> MAX_SHIFTS_IN_PERIOD, <br> MIN_REST, <br> MAX_REST, <br> NO_SHIFT_START_INTERVAL, // CIR of this type are not applicable to shifts by the api -- can not be enforced, just used by the auto scheduler <br> NO_SHIFT_END_INTERVAL, // Same as above <br> MIN_CONSECUTIVE_DAYS_OFF, // Same as above <br> MAX_CONSECUTIVE_WORKDAYS, // Same as above <br> MAX_WORKLOAD_PERCENTAGE_AT_NIGHT, // Same as above <br> MAX_SHIFT_START_VARIATION, // Same as above <br> MIN_OVERLAP_WITH_TEAM; // Same as above <br> |
| amount | `INTEGER`| A number that needs to be taken into account when setting the rule, ex: "8" min amount of working hours |
| interval_type | `STRING`| The type of interval: Either MINUTE, HOURS or DAY. |
| interval_period | `STRING`| The period of the interval: Either DAY, WEEK or MONTH |
| created_at | `TIMESTAMP`| The date and time the contract was created in Rooster in UTC |
| updated_at | `TIMESTAMP`| The date and time the contract was updated in Rooster in UTC |

### Batches

| Column | Type | Description |
| :--- | :--- | :--- |
| id | `INTEGER`| Identifier of the batch update |
| number | `INTEGER`| Batch assigned to the rider during a specific timeframe |
| active_from | `TIMESTAMP`| Datetime at which a rider's batch number has been updated |
| active_until | `TIMESTAMP`| Datetime until which a rider's batch number is valid |

### Rider Vehicles

| Column | Type | Description |
| :--- | :--- | :--- |
| vehicle_type_id | `INTEGER`| Identifier of the vehicle. |
| created_at | `TIMESTAMP`| The date and time the vehicle was created in Rooster in UTC. |
| updated_at | `TIMESTAMP`| The date and time the vehicle was updated in Rooster in UTC. |
| created_by | `INTEGER`| Identifier of user who created the vehicle. |
| updated_by | `INTEGER`| Identifier of user who updated the vehicle. |
| name | `STRING`| The name of the vehicle assigned to the rider. |
| profile | `STRING`| A profile describes whether or not it's possible to route along a particular type of way. |

### Starting Points

| Column | Type | Description |
| :--- | :--- | :--- |
| id | `INTEGER`| Identifier of the starting point that can be assigned to the rider.  |

### Rider Feedbacks

| Column | Type | Description |
| :--- | :--- | :--- |
| user_id | `INTEGER`| The numeric identifier assigned to the user giving feedback. |
| order_id | `INTEGER`| The identifier of the order generated within the logistics system. It is specific to the order within a country. |
| created_at | `TIMESTAMP` | The time and date the feedback happened. |
| reason | `STRING`| The motive related to the that rider's feedback. |
| comment | `STRING`| The actual feedback given to the rider on that order. |

### NPS

| Column | Type | Description |
| :--- | :--- | :--- |
| survey_id | `STRING`| The alphanumeric identifier assigned to the survey. |
| survey_create_datetime | `TIMESTAMP`| The date and time when the survey has been created in UTC. |
| survey_start_datetime | `TIMESTAMP` | The date and time in which the rider started the survey. |
| survey_end_datetime | `TIMESTAMP`| The date and time in which the rider finished the survey. |
| created_date | `DATE`| The date when the record has been created in UTC. |
| source_type | `STRING`| The corresponding type of survey, could be 'rider_survey' or 'new_rider_survey'. |
| response_id | `STRING`| The alphanumeric identifier assigned to the response. |
| nps_score | `INTEGER`| The global score given in the survey. Scores are between 0 to 10, where 0 means 'Not at all likely' and 10 'Extremely likely' |
| nps_reason | `STRING`| The principal area that impacted the `nps_score`. |
| rider_type | `STRING`| The classification of riders based on the nps_score. |
| is_finished | `BOOLEAN`|  Flag indicating if the survey was finished. |
| [details](#nps-details) | `ARRAY<RECORD>`| Further details of the survey answers. |

### NPS Details

| Column | Type | Description |
| :--- | :--- | :--- |
| detailed_reason | `STRING`| The area that is being evaluated in that particular question. |
| option_id | `STRING`| The option selected as answer to the question, could be a score from 1 to 10 or a string in the case of open questions. |
| answer | `STRING`| The answer to open questions. |
