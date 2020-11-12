# lg_riders

Table of riders in logistics

Clustered by field `rdbms_id`


| Name | Type | Description |
| :--- | :--- | :---        |
| id | `INTEGER` |  |
| uuid | `STRING` | Each uuid is unique and represents a rider in logistics |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| reporting_to_lg_rider_id | `INTEGER` |  |
| reporting_to_lg_rider_uuid | `STRING` |  |
| country_code | `STRING` |  |
| name | `STRING` |  |
| email | `STRING` |  |
| phone_number | `STRING` |  |
| batch_number | `INTEGER` |  |
| birth_date | `DATE` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| [contracts](#contracts) | `ARRAY<RECORD>` |  |
| [batches](#batches) | `ARRAY<RECORD>` |  |
| [custom_fields](#customfields) | `ARRAY<RECORD>` |  |
| [absences_history](#absenceshistory) | `ARRAY<RECORD>` |  |
| [vehicles](#vehicles) | `ARRAY<RECORD>` |  |
| [starting_points](#startingpoints) | `ARRAY<RECORD>` |  |
| [feedbacks](#feedbacks) | `ARRAY<RECORD>` |  |

## contracts

| Name | Type | Description |
| :--- | :--- | :---        |
| id | `INTEGER` |  |
| lg_city_id | `INTEGER` |  |
| lg_city_uuid | `STRING` |  |
| type | `STRING` |  |
| termination_type | `STRING` |  |
| status | `STRING` |  |
| name | `STRING` |  |
| job_title | `STRING` |  |
| start_at_utc | `TIMESTAMP` |  |
| end_at_utc | `TIMESTAMP` |  |
| termination_reason | `STRING` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| [interval_rules](#intervalrules) | `ARRAY<RECORD>` |  |

## interval_rules

| Name | Type | Description |
| :--- | :--- | :---        |
| id | `INTEGER` |  |
| type | `STRING` |  |
| interval_type | `STRING` |  |
| interval_period | `STRING` |  |
| amount | `INTEGER` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |

## batches

| Name | Type | Description |
| :--- | :--- | :---        |
| id | `INTEGER` |  |
| uuid | `STRING` |  |
| number | `INTEGER` |  |
| active_from_utc | `TIMESTAMP` |  |
| active_until_utc | `TIMESTAMP` |  |

## custom_fields

| Name | Type | Description |
| :--- | :--- | :---        |
| type | `STRING` |  |
| name | `STRING` |  |
| value | `STRING` |  |

## absences_history

| Name | Type | Description |
| :--- | :--- | :---        |
| id | `INTEGER` |  |
| uuid | `STRING` |  |
| lg_user_id | `INTEGER` |  |
| lg_user_uuid | `STRING` |  |
| lg_violation_id | `INTEGER` |  |
| lg_violation_uuid | `STRING` |  |
| reason | `STRING` |  |
| comment | `STRING` |  |
| is_paid | `BOOLEAN` |  |
| status | `STRING` |  |
| start_at_utc | `TIMESTAMP` |  |
| end_at_utc | `TIMESTAMP` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |

## vehicles

| Name | Type | Description |
| :--- | :--- | :---        |
| lg_vehicle_type_id | `INTEGER` |  |
| created_by_lg_user_id | `INTEGER` |  |
| created_by_lg_user_uuid | `STRING` |  |
| updated_by_lg_user_id | `INTEGER` |  |
| updated_by_lg_user_uuid | `STRING` |  |
| name | `STRING` |  |
| profile | `STRING` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |

## starting_points

| Name | Type | Description |
| :--- | :--- | :---        |
| id | `INTEGER` |  |
| uuid | `STRING` |  |

## feedbacks

| Name | Type | Description |
| :--- | :--- | :---        |
| lg_user_id | `INTEGER` |  |
| lg_user_uuid | `STRING` |  |
| lg_order_id | `INTEGER` |  |
| lg_order_uuid | `STRING` |  |
| reason | `STRING` |  |
| comment | `STRING` |  |
| created_at_utc | `TIMESTAMP` |  |
