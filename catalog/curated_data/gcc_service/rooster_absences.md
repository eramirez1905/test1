# Rooster Absences

|Column | Type | Description|
| :--- | :--- | :--- | 
|region | STRING | region|
|instance | STRING | rooster instance name|
|management_entity | STRING | management_entity|
|environment | STRING | environment code (e.g. cc-ta)|
|rdbms_id | INTEGER | environment code (e.g. cc-ta)|
|absence_id | INTEGER | id of the absence (unique per environment)|
|employee_id | INTEGER | agent id (unique per environment)|
|date_of_absence | DATE | date covered with the absence (can be multiple days per absence)|
|ordinal_day_of_absence | INTEGER | day number into the absence (1,2,3...)|
|absence_start | TIMESTAMP | datetime of the beginning of the absence|
|absence_end | TIMESTAMP | datetime of the end of the absence|
|status | STRING | status of the absence (Rejected, Pending, Accepted)|
|reason | STRING | reason for the absence (sickness, vacation, etc...)|
|paid | BOOLEAN | true if the absence is paid|
|create_unassigned_shift | BOOLEAN | true if an unassigned shift is created during absence|
|remove_agent | BOOLEAN | true if the agent must be removed during absence|
|agent_requestable | BOOLEAN | true if the agent is requestable during absence|