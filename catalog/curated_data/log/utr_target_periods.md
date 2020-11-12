# Utr Target Periods

The table lists historical changes of UTR suggestions and assumptions

| Column | Type | Description |
| :--- | :--- | :--- |
| country_code | `STRING`| A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2. |
| created_at | `TIMESTAMP` | The timestamp the record is originally created. |
| city_id | `INTEGER` | The numeric identifier of a the city. |
| timezone | `STRING`| The name of the timezone where the city is located. The timezone enables time conversion, from UTC to local time and vice versa. |
| zone_id | `INTEGER`| The numeric identifier assigned to the zone. This `id` is generated within the central logistics systems and is only unique within the same country. |
| period_name | `STRING`| The name of the period of the day when the assumptions apply. |
| weekday | `STRING`| The name of the day of the week when the assumptions apply. |
| start_time | `TIME` | The `start_time` of the UTR assumption period (Note: this time is in local time). |
| end_time | `TIME` | The `end_time` of the UTR assumption period (Note: this time is in local time). |
| [utr](#utr) | `ARRAY<RECORD>`| Records containing utr details. |

#### Utr

| Column | Type | Description |
| :--- | :--- | :--- |
| updated_at | `TIMESTAMP` | Datetime of the update.|
| suggested | `FLOAT` | Suggestion for what utr should be. |
| assumption | `FLOAT` | The latest utr assumption for the entry. |
| is_latest | `BOOLEAN` | Flag indicating if the output is the latest suggestion run by the model and corresponding latest available assumption. |
