# Zone Stats


| Column | Type | Description |
| :--- | :--- | :--- |
| country_code | `STRING`| A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2. |
| created_at_bucket | `TIMESTAMP`| The timestamp (rounded to the closest minute) when the stats are created. |
| created_date | `DATE`| The date when the stats are created. This date is based on `created_at_bucket` timestamp. |
| zone_id | `INTEGER`|	The numeric identifier assigned to the zone the stats are for. This id is generated within the central logistics systems and is only unique within the same country.|
| [stats](#stats) | `ARRAY<RECORD>`| Records pertaining the stats. |

### Stats

| Column | Type | Description |
| :--- | :--- | :--- |
| created_at | `TIMESTAMP`| The timestamp when the stat is created. |
| mean_delay | `FLOAT` | Average lateness in minutes of an order placed at this time (Used by dashboard, das, dps). |
| estimated_courier_delay | `FLOAT`|  Estimated quantile delay either provided by platforms or Hurrier calling TES while the order is being created in minutes. |
