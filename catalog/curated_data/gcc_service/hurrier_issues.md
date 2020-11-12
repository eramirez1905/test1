# Hurrrier Issues

|Column | Type | Description|
| :--- | :--- | :--- | 
|source_id | INTEGER | Unique identifier of the entity (country + brand)|
|country | STRING | Country name|
|region | STRING | Region|
|contact_center | STRING | Contact Center|
|management_entity | STRING | Management Entity to which the entity is attached to legally|
|management_entity_group | STRING | Group of entities (for Tableau permissions)|
|global_entity_id | STRING | global entity id of the country/brand from which the issue originated|
|dispatch_rdbms_id | INTEGER | Unique backend identifier for Logistics backend (1-to-1 relationship with log_country_code)|
|hurrier_issue_id | STRING | The numeric identifier of an issue within Hurrier.|
|country_code | STRING | A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2.|
|delivery_id | INTEGER | The numeric identifier of a delivery that issue can be linked to.|
|rider_id | INTEGER | The numeric identifier of a rider that issue can be linked to.|
|city_id | INTEGER | The numeric identifier assigned to every city|
|issue_type | STRING | The issue type refers to either in which delivery leg the issue occured (DropoffIssue, PickupIssue) or which party is responsible for the issue (DispatchIssue, CourierIssue).|
|issue_category | STRING | The issue category represents a sub-category of the issue type.|
|category_l1 | STRING | The level 1 category defined for the issue type|
|created_at | TIMESTAMP | The date and time the issue was created within Hurrier.|
|picked_at | TIMESTAMP | The date and time the issue was picked (to work on) by a dispatcher.|
|resolved_at | TIMESTAMP | The date and time the issue was either manually resolved by a dispatcher or got solved by the rider.|
|wait_time_picked_secs | FLOAT | Wait time of the picked issue in seconds (from creation to picked)|
|wait_time_non_picked_secs | FLOAT | Wait time of the non-picked issue in seconds (from creation to resolved. resolution is based on rules set for the issue type in Hurrier)|
|handling_time_secs | FLOAT | Handling time of the issue in seconds (from picked to resolved)|
|issue_picked_within_sla_30s | INTEGER | Issue got picked in less than 30 sec|
|issue_picked_within_sla_60s | INTEGER | N/A|
|issue_picked_within_sla_300s | INTEGER | N/A|
|dismissed_at | TIMESTAMP | The date and time the issue was manually dismissed by a dispatcher.|
|show_on_watchlist | BOOLEAN | Flag whether the issue was shown to the dispatcher by the newly introduced service Issue Customization Engine ICE. Only issues that are customized in ICE and were pushed to the issue watchlist will show TRUE.|
|picked_by | STRING | The dispatcher who worked on the issue.|
|updated_at | TIMESTAMP | The date and time of the last update to the issue.|