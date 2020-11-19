# Shifts


| Column | Type | Description |
| :--- | :--- | :--- |
| country_code | `STRING`| A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2. |
| created_date | `DATE`| Planned start date of the shift (**Partition column**) |
| shift_id | `INTEGER`| Id of the shift in Rooster  |
| rider_id | `INTEGER`| Identifier of the rider used by Rooster |
| city_id | `INTEGER`| Id used by logistics to identify the city of the shift within a country.  |
| timezone | `STRING`| The name of the timezone where the city is located. The timezone enables time conversion, from UTC to local time. |
| is_repeating | `BOOLEAN`| N/A |
| starting_point_id | `INTEGER`| Id of starting point the shift is assigned to in Rooster |
| zone_id | `INTEGER`|	The numeric identifier assigned to the zone. This id is generated within the central logistics systems and is only unique within the same country.|
| shift_start_at | `TIMESTAMP`| Planned start time of the shift  |
| shift_end_at | `TIMESTAMP`| Planned end time of the shift |
| planned_shift_duration | `INTEGER`| The scheduled duration (in seconds) of the shift
| [absences](#absences) | `ARRAY<RECORD>`| Records containing information of absences in **ACCEPTED** status overlapping the planned start and end time of the shift. |
| shift_state | `STRING`| state of the shift: **evaluated**, **in discussion**, **published**, **pending**, **cancelled**, **no show** |
| shift_tag | `STRING`| application mode to shift |
| actual_start_at | `TIMESTAMP`| Actual start time of the shift (when the rider logged in) |
| actual_end_at | `TIMESTAMP`| Actual end time of the shift (when the rider logged out) |
| login_difference | `INTEGER`| Difference in seconds from actual start time of the shift and planned start time |
| logout_difference | `INTEGER`| Difference in seconds from actual end time of the shift and planned end time |
| [deliveries](#deliveries) | `<RECORD>`| Record containing deliveries info within the worked shift|
| actual_working_time | `INTEGER`| Difference in seconds of the actual end time of the shift and actual start time of the shift. |
| [actual working time by day](#actual-working-time-by-day) | `ARRAY<RECORD>` | Evaluations working time by day record. |
| actual_break_time | `INTEGER`| The time in seconds the rider was put on break by a dispatcher or automatically. Break durations are a non paid status. |
| [actual break time by day](#actual-break-time-by-day) | `ARRAY<RECORD>` | Evaluations break time by day record. |
| [evaluations](#evaluations) | `ARRAY<RECORD>`| N/A |
| [break_time](#break-time) | `ARRAY<RECORD>`| N/A |
| shift_created_by | `INTEGER `| Id of the user who created the shift (0 if automatic) |
| shift_updated_by | `INTEGER `| Id of the user who updated the shift (0 if automatic) |
| created_at | `TIMESTAMP`| The time and date the shift was created. |
| updated_at | `TIMESTAMP`| The time and date the shift was updated. |
| vehicle_bag | `STRING`| The combination of Hurrier vehicle name and bag used by the rider during the shift day. In case of multiple vehicles used, the one with the most orders delivered is taken into account. Field is `NULL` for future shifts as information is still unknown. |
| vehicle_profile | `STRING` | The google profile used to estimate the driving times to the vendor and the customer. |

### Absences

Note: Through the following fields the user can check if there was an ACCEPTED absence when shift_state is a NO_SHOW.

| Column | Type | Description |
| :--- | :--- | :--- |
| id | `INTEGER`| The unique identifier of the absence used by Rooster. |
| start_at | `TIMESTAMP`| The start datetime of the absence. |
| end_at | `TIMESTAMP`| The end datetime of the absence. |
| reason | `STRING`| The explanation of why an absence is created.(Eg: Sickness). |
| status | `STRING`| Current status of the absence.(Eg: Accepted, Pending, Rejected). |
| is_paid | `BOOLEAN`| Flag indicating if the absence is paid or not. |
| comment | `STRING`| Any additional comment on the reason of the absence. |
| violation_id | `INTEGER`| The identifier of the compliance violation triggering the absence. |

### Deliveries

| Column | Type | Description |
| :--- | :--- | :--- |
| accepted | `INTEGER`| The number of deliveries the rider accepted during the evaluated shift |
| notified | `INTEGER`| The number of deliveries the rider got notified for during the evaluated shift |

### Evaluations

A single shift can have multiple evaluations in case the rider was put on break either manually or automatically.

| Column | Type | Description |
| :--- | :--- | :--- |
| id | `INTEGER`| Id identifying the Rooster evaluation of the shift |
| status | `STRING`| Result of the evaluation: **rejected**, **pending**, **accepted**, **cancelled** |
| start_at_local | `DATETIME`| Timestamp of the beginning of the evaluation in local time. |
| end_at_local | `DATETIME`| Timestamp of the end of the evaluation in local time. |
| start_at | `TIMESTAMP`| Timestamp of the beginning of the evaluation |
| end_at | `TIMESTAMP`| Timestamp of the end of the evaluation |
| duration | `INTEGER`| The duration in seconds of this particular shift evaluation in the day the shift started. |
| [vehicle](#vehicle) | `RECORD`| Information pertaining the vehicle used during the evaluated shift. |

### Vehicle

| Column | Type | Description |
| :--- | :--- | :--- |
| name | `STRING` | The name of the vehicle used during the evaluated shift. |

### Break time

| Column | Type | Description |
| :--- | :--- | :--- |
| start_at_local | `DATETIME`| Timestamp of the beginning of the break in local time. |
| end_at_local | `DATETIME`| Timestamp of the end of the break in local time. |
| start_at | `TIMESTAMP`| Timestamp of the beginning of the break |
| end_at | `TIMESTAMP`| Timestamp of the end of the break |
| [details](#details) | `RECORD`| N/A |
| duration | `INTEGER`| The duration in seconds the break time lasted in the day the shift started. |

### Details

| Column | Type | Description |
| :--- | :--- | :--- |
| performed_by | `STRING`| The source the break was initiated from: **dispatcher**, **courier**, **issue_service** |
| type | `STRING`| Indicator whether break was created manually or automaticcaly. |
| reason | `STRING`| Reason given by the initiator of the break |
| comment | `STRING`| Additional, but optional information given by the initiator of the break. |

### Actual working time by day

| Column | Type | Description |
| :--- | :--- | :--- |
| day | `DATE`| Date of the start of the evaluation in local time. |
| duration | `INTEGER`| The duration in seconds the evaluation time lasted in the day it started. |
| status | `STRING`| Result of the evaluation: **rejected**, **pending**, **accepted**, **cancelled** |
| vehicle_name | `STRING` | The name of the vehicle used during the evaluated shift. |
| notified_deliveries_count | `INTEGER` | The number of deliveries the rider got notified for during the evaluated shift. **Field is deprecated and set to `NULL`. Do not use it** |
| accepted_deliveries_count | `INTEGER` | The number of deliveries the rider accepted during the evaluated shift. **Field is deprecated and set to `NULL`. Do not use it** |

### Actual break time by day

| Column | Type | Description |
| :--- | :--- | :--- |
| day | `DATE`| Date of the start of the break in local time. |
| duration | `INTEGER`| The duration in seconds the break time lasted in the day it started. |
