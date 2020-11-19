# Lost Orders

This table has the purpose of showing all the lost orders related to events in Porygon

| Column | Type | Description |
| :--- | :--- | :--- |
| country_code | `STRING`| A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2. |
| created_date | `DATE` | The date when the records refer to.|
| event_type | `STRING` | The event that triggered the lost orders (Eg: `close`, `shrink`). |
| zone_id | `INTEGER`| The numeric identifier assigned to the zone. This `id` is generated within the central logistics systems and is only unique within the same country. |
| city_id | `INTEGER` | The numeric identifier assigned to every city |
| timezone | `STRING`| The name of the timezone where the city is located. The timezone enables time conversion, from UTC to local time and vice versa. |
| [lost_orders](#lost-orders) | `ARRAY<RECORD>`| Records containing details about lost orders. |
| [timings](#timings) | `<RECORD>`| Record containing the `MIN` start and `MAX` end time of the lost orders period from the `events`. |
| [events](#events) | `ARRAY<RECORD>`| Records containing details about events. |

### Lost Orders

| Column | Type | Description |
| :--- | :--- | :--- |
| interval_start | `TIMESTAMP` | The closest 30 minutes interval start of the lost orders. |
| starts_at | `TIMESTAMP` | The start time of the lost orders. |
| ends_at | `TIMESTAMP` | The end time of the lost orders. |
| estimate | `INTEGER` | The initial number of lost orders based on the the raw prediction in the event timeframe |
| net | `INTEGER` | The final number of lost orders predicted in the the event timeframe. |

### Timings

| Column | Type | Description |
| :--- | :--- | :--- |
| starts_at | `TIMESTAMP` | The start of event that triggers the lost orders. |
| ends_at | `TIMESTAMP` | The end of the event tha triggers the lost orders. |

### Events

This `ARRAY` contains of all the detailed events and transactions that triggered the lost orders

| Column | Type | Description |
| :--- | :--- | :--- |
| id | `INTEGER` | The numeric identifier of an event in `Porygon`. |
| transaction_id | `INTEGER` | The numeric identifier of the beginning of the activation of an event. |
| end_transaction_id | `INTEGER` | The numeric identifier of the beginning of the deactivation of an event. |
| starts_at | `TIMESTAMP` | The start of the `event_id` and `transaction_id` that triggers the lost orders. |
| ends_at | `TIMESTAMP` | The end of the `event_id` and `transaction_id` triggers the lost orders. |
