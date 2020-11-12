# lg_daily_rider_zone_kpi

Rider kpi aggregated by rider, date, zone, batch, vehicle name,

Partitioned by field `created_date_local` with each
partition being a time interval of `DAY`

Clustered by fields `rdbms_id`, `lg_rider_uuid`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid represents an aggregation of rider kpi by rider, date, zone, batch, vehicle name. So this should not be used for any JOIN |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| lg_city_uuid | `STRING` |  |
| lg_city_id | `INTEGER` |  |
| lg_zone_uuid | `STRING` |  |
| lg_zone_id | `INTEGER` |  |
| lg_rider_uuid | `STRING` | Each uuid represents a rider in logistics |
| lg_rider_id | `INTEGER` |  |
| contract_type | `STRING` |  |
| country_code | `STRING` |  |
| country_name | `STRING` |  |
| city_name | `STRING` |  |
| zone_name | `STRING` |  |
| rider_name | `STRING` |  |
| email | `STRING` |  |
| batch_number | `INTEGER` |  |
| current_batch_number | `INTEGER` |  |
| contract_name | `STRING` |  |
| job_title | `STRING` |  |
| rider_contract_status | `STRING` |  |
| captain_name | `STRING` |  |
| vehicle_profile | `STRING` |  |
| vehicle_name | `STRING` |  |
| tenure_in_weeks | `FLOAT` |  |
| tenure_in_years | `FLOAT` |  |
| start_and_first_shift_diff_in_days | `INTEGER` |  |
| shifts_done_count | `INTEGER` |  |
| late_shift_count | `INTEGER` |  |
| all_shift_count | `INTEGER` |  |
| no_show_count | `INTEGER` |  |
| unexcused_no_show_count | `INTEGER` |  |
| weekend_shift_count | `INTEGER` |  |
| transition_working_time_in_seconds | `FLOAT` |  |
| transition_busy_time_in_seconds | `FLOAT` |  |
| peak_time_in_seconds | `INTEGER` |  |
| working_time_in_seconds | `INTEGER` |  |
| planned_working_time_in_seconds | `INTEGER` |  |
| break_time_in_seconds | `INTEGER` |  |
| swaps_accepted_count | `INTEGER` |  |
| swaps_pending_no_show_count | `INTEGER` |  |
| swaps_accepted_no_show_count | `INTEGER` |  |
| created_date_local | `DATE` |  |
| contract_start_date_local | `DATE` |  |
| contract_end_date_local | `DATE` |  |
| contract_creation_date_local | `DATE` |  |
| hiring_date_local | `DATE` |  |
