# Applicants (BETA version)

## Table structure may change without any prior notice, use at your own risk.

This table contains the applicants from Arara 

| Column | Type | Description |
| :--- | :--- | :--- |
| region | `STRING`| The two-letter code of the operational region in which the country is located. The three operational regions are America (US), Europe and Middle East (EU), Asia (AP). |
| country_code | `STRING`| A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2. |
| timezone | `STRING`| The name of the timezone where the country is located. The timezone enables time conversion, from UTC to local time. |
| location_id | `STRING`| The Location ID of the applicant in the format country_code:city_name:location. |
| applicant_id | `STRING`| Unique internal id of an applicant in Arara. |
| created_date | `DATE`| The Date of when the applicant was created in Arara. |
| created_at | `TIMESTAMP`| The Timestamp of when the applicant was created in Arara. |
| city_id | `INTEGER`| The numeric identifier assigned to every city. This id is generated within the central logistics systems and is only unique within the same country. |
| location_name | `STRING`| The location of the applicant. |
| language_id | `STRING`| A two-character code based on the language chosen by the applicant on the landing page as specified in ISO-639-1 |
| duplicate | `BOOLEAN`| Upon creation, Arara marks applicants with already known email addresses or phone numbers as duplicates. Keep in mind, that the duplicate of the applicant may be the application which has the current progress of the applicant. Useful for getting more accurate total numbers of applicants. |
| rider_id | `INTEGER`| After the rooster account is created the employee id is stored in Arara.  |
| braze_user_created_at | `TIMESTAMP`| After the applicant is created, we also subsequently create a user in Braze. |
| braze_welcome_sent | `BOOLEAN`| Whether the welcome message was sent to an applicant |
| last_reminded_at | `TIMESTAMP`| Workflows allow the configuration of intervals, at which reminder messages are sent to idle candidates. last_reminded_at logs the last time that such a message was sent to an applicant.  |
| rejected_at | `TIMESTAMP`| Timestamp of the rejection of the applicant. |
| rejected_by | `STRING` | Applicants can be either rejected by `auto_rule` if they were rejected based on automated checks, `idle_time` if they were rejected based on their inactivity or manually by a recruiter, in which case we log the email of the recruiter. It is available for all applicants rejected after 2020-04-07.|
| rejection_category | `STRING`| Rejection Category helps to classify whether a candidate withdrew his application or whether he was rejected. May help to better understand conversion rates of a given city. |
| rejection_type | `STRING`| Detailed reason of the failure of the application. |
| approved_at | `TIMESTAMP`| Timestamp fo the approval of the applicant. |
| referred_by_id | `INTEGER`| rider_id of the rider who referred the applicant. |
| [custom_fields](#custom-fields) | `RECORD` | Custom fields record. |
| [workflow](#workflow) | `RECORD` | Workflow record. |
| url_id | `STRING` | Hash that identifies the short URL. |
| url | `STRING` | Full URL where an applicant is redirected. |
| link_click_count | `STRING` | Amount of times an applicant is redirected to the full URL. |
| link_type | `STRING` | Type of the target URL. Currently this can either be "Portal" (Arara portal) or "Unknown" type (eg. testing urls). |

## Custom fields

| Column | Type | Description |
| :--- | :--- | :--- |
| type | `STRING`| Data type of the field |
| name | `STRING`| Contains all additional applicant information, which is not subject to gdpr constraints. Currently the following fields are whitelisted: <br> `utm_medium`, `utm_source`, `utm_term`, `utm_content`, `utm_campaign`, `city`,  `area`, `country`, `language`, `vehicle`, `age_check`, `vehicle_check`, `gender`, `app_user`, `bot_user`, `source`. |
| value | `STRING`| The content of the field |

## Workflow

| Column | Type | Description |
| :--- | :--- | :--- |
| name | `STRING` | Name of the workflow |
| max_idle_days | `INTEGER` | #days after which an applicant is automatically rejected with reason "unresponsive". |
| idle_reminder_delay | `INTEGER` | #days interval after which idle applicants receive reminder messages to reengage in their application |
| [tier](#tiers) | `RECORD` | Tier record. |

## Tiers

| Column | Type | Description |
| :--- | :--- | :--- |
| Tier | `INTEGER` | Tiers define the order of stages in a workflow. Stages with the same tier_index become available at the same time, and are accessible to the applicant simultaneously. |
| [stage](#stages) | `RECORD` | Applicant Stages record. |

## Stages

| Column | Type | Description |
| :--- | :--- | :--- |
| name | `STRING` | N/A. |
| type| `STRING`| N/A. |
| index_in_tier | `INTEGER` | The index in tier defines the default order stages are presented to applicants within a tier.  |  
| [transitions](#transitions) | `RECORD` | Transitions record. |


## Transitions

| Column | Type | Description |
| :--- | :--- | :--- |
| to_state| `STRING`| Stages can have various states: Locked, Available, In review. Locked: All stages of a tier need to be completed for a candidate to see stages of the next tier. All stages in higher tiers are locked. Applicants who are automatically rejected in our system will not have transitions to "available" and thus no timestamps for any transitions. Transitions to available are created automatically upon applicant creation for final stages (Tier 1002). For approval and rejection timestamps use approved at and rejected at in applicant record.  |
| created_at | `TIMESTAMP` | Timestamp of the transition.  |
