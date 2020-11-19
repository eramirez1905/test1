# lg_shifts

Table of shifts in logistics.

Partitioned by field `created_date_utc` with each
partition being a time interval of `DAY`

Clustered by fields `rdbms_id`, `lg_rider_uuid`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid is unique and represents a shift |
| id | `INTEGER` |  |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| lg_rider_uuid | `STRING` | Each uuid represents a rider in logistics |
| lg_rider_id | `INTEGER` |  |
| lg_city_uuid | `STRING` |  |
| lg_city_id | `INTEGER` |  |
| lg_starting_point_uuid | `STRING` |  |
| lg_starting_point_id | `INTEGER` |  |
| lg_zone_uuid | `STRING` |  |
| lg_zone_id | `INTEGER` |  |
| created_by_lg_user_uuid | `STRING` |  |
| created_by_lg_user_id | `INTEGER` |  |
| updated_by_lg_user_uuid | `STRING` |  |
| updated_by_lg_user_id | `INTEGER` |  |
| country_code | `STRING` |  |
| is_repeating | `BOOLEAN` |  |
| state | `STRING` |  |
| tag | `STRING` |  |
| vehicle_bag | `STRING` |  |
| vehicle_profile | `STRING` |  |
| deliveries_accepted_count | `INTEGER` |  |
| deliveries_notified_count | `INTEGER` |  |
| planned_shift_duration_in_seconds | `INTEGER` |  |
| login_difference_in_seconds | `INTEGER` |  |
| logout_difference_in_seconds | `INTEGER` |  |
| actual_working_time_in_seconds | `INTEGER` |  |
| actual_break_time_in_seconds | `INTEGER` |  |
| timezone | `STRING` |  |
| start_at_utc | `TIMESTAMP` |  |
| end_at_utc | `TIMESTAMP` |  |
| actual_start_at_utc | `TIMESTAMP` |  |
| actual_end_at_utc | `TIMESTAMP` |  |
| created_date_utc | `DATE` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| [absences](#absences) | `ARRAY<RECORD>` |  |
| [actual_working_time_by_date](#actualworkingtimebydate) | `ARRAY<RECORD>` |  |
| [actual_break_time_by_date](#actualbreaktimebydate) | `ARRAY<RECORD>` |  |
| [evaluations](#evaluations) | `ARRAY<RECORD>` |  |
| [break_times](#breaktimes) | `ARRAY<RECORD>` |  |

## absences

| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` |  |
| id | `INTEGER` |  |
| lg_violation_uuid | `STRING` |  |
| lg_violation_id | `INTEGER` |  |
| status | `STRING` |  |
| reason | `STRING` |  |
| comment | `STRING` |  |
| is_paid | `BOOLEAN` |  |
| start_at_utc | `TIMESTAMP` |  |
| end_at_utc | `TIMESTAMP` |  |

## actual_working_time_by_date

| Name | Type | Description |
| :--- | :--- | :---        |
| vehicle_name | `STRING` |  |
| deliveries_accepted_count | `INTEGER` |  |
| deliveries_notified_count | `INTEGER` |  |
| status | `STRING` |  |
| date_utc | `DATE` |  |

## actual_break_time_by_date

| Name | Type | Description |
| :--- | :--- | :---        |
| duration_in_seconds | `INTEGER` |  |
| date_utc | `DATE` |  |

## evaluations

| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` |  |
| id | `INTEGER` |  |
| status | `STRING` |  |
| vehicle_name | `STRING` |  |
| duration_in_seconds | `INTEGER` |  |
| start_at_local | `DATETIME` |  |
| end_at_local | `DATETIME` |  |
| start_at_utc | `TIMESTAMP` |  |
| end_at_utc | `TIMESTAMP` |  |

## break_times

| Name | Type | Description |
| :--- | :--- | :---        |
| performed_by_type | `STRING` |  |
| type | `STRING` |  |
| reason | `STRING` |  |
| comment | `STRING` |  |
| duration_in_seconds | `INTEGER` |  |
| start_at_local | `DATETIME` |  |
| end_at_local | `DATETIME` |  |
| start_at_utc | `TIMESTAMP` |  |
| end_at_utc | `TIMESTAMP` |  |
