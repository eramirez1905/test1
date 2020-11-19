# Rooster Shifts

|Column | Type | Description|
| :--- | :--- | :--- | 
|rdbms_id | INTEGER | rooster instance id|
|region | STRING | region|
|instance | STRING | rooster instance name|
|management_entity | STRING | N/A|
|environment | STRING | environment code (e.g. cc-ta)|
|shift_id | INTEGER | id of the shift (unique per environment)|
|start_at | TIMESTAMP | datetime of the scheduled start of the shift (local time)|
|end_at | TIMESTAMP | datetime of the scheduled end of the shift (local time)|
|state | STRING | state of the shift|
|schedule_time_zone | STRING | timezone in which the shift happened|
|shift_tag | STRING | N/A|
|parent_id | INTEGER | N/A|
|updated_by | INTEGER | N/A|
|evaluation_start_at | TIMESTAMP | datetime of the evlauated (clocked) start of the shift (local time)|
|evaluation_end_at | TIMESTAMP | datetime of the evlauated (clocked) end of the shift (local time)|
|evaluation_id | INTEGER | related evaluation of the shift|
|evaluation_status | STRING | status of the related evaluation|
|employee_id | INTEGER | agent id (unique per environment)|
|email | STRING | agent email|
|team_lead_id | INTEGER | team lead id (unique per environment)|
|team_lead_email | STRING | team lead email|
|contract_type | STRING | type of contract of the agent who did the shift|
|position_id | INTEGER | position id (nesting/senior agents)|
|team_id | INTEGER | id of the team|
|team_name | STRING | name of the team (e.g. Amber)|
|department_id | INTEGER | N/A|
|department_name | STRING | N/A|
|area_responsibility | STRING | management entity (e.g. Talabat)|
|office_name | STRING | locol office where the agents of the team are sitting|
|bpo | BOOLEAN | name of the BPO in case the sub-team belongs to a BPO|
|setup | STRING | setup of the sub-team: CB (country-based) or LB (language-based)|
|department | STRING | N/A|
|hours_scheduled | FLOAT | hours scheduled for the related shift|
|hours_clocked | FLOAT | hours clocked/evaluated for the related shift|
|hours_clocked_within_schedule | FLOAT | N/A|
|mins_scheduled | FLOAT | N/A|
|mins_clocked | FLOAT | N/A|
|break_duration | FLOAT | scheduled duration of the break for the related shift|
|hours_clocked_net | FLOAT | hours clocked/evaluated net of break for the related shift|
|attended_agent | INTEGER | 1 if the shift was clocked/evaluated|
|no_show | INTEGER | 1 if the agent did not show up for the shift|
|created_at | TIMESTAMP | datetime of shift creation|
|updated_at | TIMESTAMP | datetime of shift last update|
|version | INTEGER | latest version of the shift|
|zone_id | INTEGER | N/A|
|roll_out_date | DATE | N/A|
|reporting_flag | INTEGER | N/A|