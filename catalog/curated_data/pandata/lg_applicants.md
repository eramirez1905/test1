# lg_applicants

Table of applicants for riders

Partitioned by field `created_date_utc` with each
partition being a time interval of `DAY`

Clustered by field `rdbms_id`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` |  |
| id | `STRING` | Each id is unique and represents an applicant |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| referrer_lg_rider_uuid | `STRING` |  |
| referrer_lg_rider_id | `INTEGER` |  |
| lg_city_uuid | `STRING` |  |
| lg_city_id | `INTEGER` |  |
| lg_language_id | `STRING` |  |
| lg_rider_uuid | `STRING` |  |
| lg_rider_id | `INTEGER` |  |
| url_id | `STRING` |  |
| region | `STRING` |  |
| country_code | `STRING` |  |
| is_duplicate | `BOOLEAN` |  |
| is_braze_welcome_sent | `BOOLEAN` |  |
| rejection_category_type | `STRING` |  |
| rejection_type | `STRING` |  |
| link_type | `STRING` |  |
| url | `STRING` |  |
| link_click_count | `STRING` |  |
| timezone | `STRING` |  |
| created_date_utc | `DATE` |  |
| created_at_local | `TIMESTAMP` |  |
| created_at_utc | `TIMESTAMP` |  |
| braze_user_created_at_local | `TIMESTAMP` |  |
| braze_user_created_at_utc | `TIMESTAMP` |  |
| last_reminded_at_local | `TIMESTAMP` |  |
| last_reminded_at_utc | `TIMESTAMP` |  |
| rejected_at_local | `TIMESTAMP` |  |
| rejected_at_utc | `TIMESTAMP` |  |
| approved_at_local | `TIMESTAMP` |  |
| approved_at_utc | `TIMESTAMP` |  |
| [custom_fields](#customfields) | `ARRAY<RECORD>` |  |
| [workflow](#workflow) | `RECORD` |  |

## custom_fields

| Name | Type | Description |
| :--- | :--- | :---        |
| type | `STRING` |  |
| name | `STRING` |  |
| value | `STRING` |  |

## workflow

| Name | Type | Description |
| :--- | :--- | :---        |
| name | `STRING` |  |
| max_idle_in_days | `INTEGER` |  |
| workflow_idle_reminder_delay_in_days | `INTEGER` |  |
| [tiers](#tiers) | `ARRAY<RECORD>` |  |

## tiers

| Name | Type | Description |
| :--- | :--- | :---        |
| tier | `INTEGER` |  |
| [stages](#stages) | `ARRAY<RECORD>` |  |

## stages

| Name | Type | Description |
| :--- | :--- | :---        |
| index_in_tier | `INTEGER` |  |
| name | `STRING` |  |
| type | `STRING` |  |
| [transitions](#transitions) | `ARRAY<RECORD>` |  |

## transitions

| Name | Type | Description |
| :--- | :--- | :---        |
| to_state | `STRING` |  |
| created_at_utc | `TIMESTAMP` |  |
| created_at_local | `TIMESTAMP` |  |
