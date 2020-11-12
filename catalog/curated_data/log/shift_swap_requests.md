# Shift Swap Requests

This table aggregates all shift swap requests done by the rider with their statuses. 

| Column | Type | Description |
| :--- | :--- | :--- |
| region | `STRING`| The operational region in which the country is located. |
| country_code | `STRING`| A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2. |
| created_date | `DATE`| Date when the request is made by the rider (**Partition column**) |
| created_by | `INTEGER`| Identifier of the rider who created the request used by Rooster |
| shift_id | `INTEGER`| Identifier of the shift in Rooster  |
| timezone | `STRING`| The name of the timezone where the city is located. The timezone enables time conversion, from UTC to local time. |
| [swap_requests](#swap-requests) | `ARRAY<RECORD>`| Array indicating details of all the swap requests by the shift. |


## Swap Requests

| Column | Type | Description |
| :--- | :--- | :--- |
| id | `STRING`| Identifier of the request |
| status | `STRING`| State of the swap request: <br>`PENDING`: Rider offers a shift swap, but shift is still awaiting for an acceptance from another rider <br>`CANCELLED`: Rider offers a shift swap, but for any reason the shift gets cancelled <br>`REJECTED`: Rider offers a shift swap, but then rider decides to withdraw the offer <br>`ACCEPTED`: Rider offers a shift swap, and another rider accepted the offer. |
| accepted_by | `INTEGER`| Identifier of the rider who accepted the request used in Rooster. |
| created_at | `TIMESTAMP`| Timestamp indicating when the request was created by the rider. |
| accepted_at | `TIMESTAMP`| Timestamp indicating when the request was accepted by a rider. |
| shift_start_at | `TIMESTAMP`| Planned start time of the shift. |
| shift_end_at | `TIMESTAMP`| Planned end time of the shift. |
| starting_point_id | `INTEGER`| Id of starting point the shift is assigned to in Rooster |
