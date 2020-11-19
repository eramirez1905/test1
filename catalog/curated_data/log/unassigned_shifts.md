# Unassigned Shifts


This table shows all the unassigned shifts.
The `unassigned_shift_id` is unique only within a `country_code`.

| Column | Type | Description |
| :--- | :--- | :--- |
| country_code | `STRING`| A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2. |
| city_id | `INTEGER`| The numeric identifier assigned to the city. This id is only unique within the same country. |
| zone_id | `INTEGER`|	The numeric identifier assigned to the zone. This id is generated within the central logistics systems and is only unique within the same country. |
| starting_point_id | `INTEGER`| Id of starting point the shift is assigned to in Rooster. |
| unassigned_shift_id | `INTEGER`| The id of the unassigned shift. |
| slots | `INTEGER`| The number of slots in that unassigned shift. |
| start_at | `TIMESTAMP`| Datetime when the shift starts. |
| end_at | `TIMESTAMP`| Datetime when the shift ends. |
| created_at | `TIMESTAMP`| Datetime when the shift was created. |
| updated_at | `TIMESTAMP`| Datetime when the shift was updated. |
| created_by | `INTEGER`| The id of the shift's creator. If it is `0`, the shift was created automatically.  |
| updated_by | `INTEGER`| The id of the person that updated the shift. If it is `0`, the shift was updated automatically.  |
| tag | `STRING`| How the unassigned shift is created: **AUTOMATIC, COPIED, TERMINATION, STARTING_POINT_UNASSIGNED, MANUAL** and **SWAP**. |
| state | `STRING`| State of the unassigned shift: **PUBLISHED, PENDING** and **CANCELLED**. |
| timezone | `STRING`| The name of the timezone where the city is located. The timezone enables time conversion, from UTC to local time. |
| created_date | `DATE` | Date when the unassigned shift is created in Rooster (Partition column). |
