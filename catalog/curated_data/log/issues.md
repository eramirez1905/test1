# Issues

Issues are created automatically from Hurrier and are visualized on the Hurrier dashboard for dispatchers to act upon

An issue can be either linked to:
 - a delivery (e.g. exceeding distance limitations making it an undispatchable order)
 - a rider (e.g. a rider that is out of his current zone)
 - a delivery and a rider (e.g. a delivery that has not been accepted by a rider within x minutes)

There can be multiple issues per delivery and/or rider.

| Column | Type | Description |
| :--- | :--- | :--- |
| country_code | `STRING`| A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2. |
| created_date | `DATE` | N\A (Partition column) |
| issue_id | `STRING` | The numeric identifier of an issue within Hurrier. |
| delivery_id | `INTEGER` | The numeric identifier of a delivery that issue can be linked to. |
| rider_id | `INTEGER` | The numeric identifier of a rider that issue can be linked to. |
| city_id | `INTEGER` | The numeric identifier assigned to every city |
| zone_id | `INTEGER` | The numeric identifier assigned to every zone |
| timezone | `STRING` | The name of the timezone where the city is located. The timezone enables time conversion, from UTC to local time. |
| issue_type | `STRING` | The issue type refers to either in which delivery leg the issue occured (`DropoffIssue`, `PickupIssue`) or which "party" is responsible for the issue (`DispatchIssue`, `CourierIssue`). |
| notes | `STRING`| Additional specifications on the issue_type and issue_category. |
| [actions](#actions) | `ARRAY<RECORD>` | Automatic actions set up via Issue Service. |
| issue_category | `STRING` | The issue category represents a sub-category of the issue type. |
| issue_name | `STRING` | Naming assigned to the issue according to issue service. It's based on issue_type and issue_category. |
| picked_at | `TIMESTAMP` | The date and time the issue was picked (to work on) by a dispatcher. |
| picked_by | `TIMESTAMP` | The dispatcher who worked on the issue. |
| dismissed_at | `TIMESTAMP` | The date and time the issue was manually dismissed by a dispatcher. As this information doesn't exists in the new Issue Service, expect to see a change in numbers as different issue types migrate from Hurrier to Issue Service. |
| resolved_at | `TIMESTAMP` | The date and time the issue was either manually resolved by a dispatcher or got solved by the rider. |
| shown_on_watchlist | `BOOLEAN`| Flag whether the issue was shown to the dispatcher by the newly introduced service Issue Customization Engine `ICE`. Only issues that are customized in ICE and were pushed to the issue watchlist will show `TRUE`. |
| created_at | `TIMESTAMP` | The date and time the issue was created within Hurrier. |
| updated_at | `TIMESTAMP` | The date and time of the last update to the issue. | 

## Actions

Automatic actions set up via Issue Service 

| Column | Type | Description |
| :--- | :--- | :--- |
| action_id | `STRING` | Unique identifier of the issue. |
| time_to_trigger_minutes | `INTEGER` | Time that has to pass after issue creation before action is taken. |
| action | `STRING` | Qualitative description of the action e.g. Put rider on break. |
| action_value | `STRING` | Quantitative description of the action e.g. How many minutes a rider is put on break. |
| status | `STRING` | Indicator whether action was successful. |
| created_at | `TIMESTAMP` | The date and time the action was executed. |
| updated_at | `TIMESTAMP` | The date and time the action was updated. |
| [condtions](#conditions) | `ARRAY<RECORD>` | Additional condition that needs to be true for the action to be executed. | 

## Conditions

| Column | Type | Description |
| :--- | :--- | :--- |
| type | `STRING` | Qualitative description of the condition e.g. issue_occurred_x_consecutive_times. |
| value | `STRING` | Quantitative description of the condition e.g. how many consecutive times. |
