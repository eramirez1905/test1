SELECT
  rider_kpi.uuid,
  countries.rdbms_id,
  rider_kpi.lg_city_uuid,
  rider_kpi.lg_city_id,
  rider_kpi.lg_zone_uuid,
  rider_kpi.lg_zone_id,
  rider_kpi.lg_rider_uuid,
  rider_kpi.lg_rider_id,
  rider_kpi.contract_type,
  rider_kpi.country_code,
  rider_kpi.country_name,
  rider_kpi.city_name,
  rider_kpi.zone_name,
  rider_kpi.rider_name,
  rider_kpi.email,
  rider_kpi.batch_number,
  rider_kpi.current_batch_number,
  rider_kpi.contract_name,
  rider_kpi.job_title,
  rider_kpi.rider_contract_status,
  rider_kpi.captain_name,
  rider_kpi.vehicle_profile,
  rider_kpi.vehicle_name,
  rider_kpi.tenure_in_weeks,
  rider_kpi.tenure_in_years,
  rider_kpi.start_and_first_shift_diff_in_days,
  rider_kpi.shifts_done_count,
  rider_kpi.late_shift_count,
  rider_kpi.all_shift_count,
  rider_kpi.no_show_count,
  rider_kpi.unexcused_no_show_count,
  rider_kpi.weekend_shift_count,
  rider_kpi.transition_working_time_in_seconds,
  rider_kpi.transition_busy_time_in_seconds,
  rider_kpi.peak_time_in_seconds,
  rider_kpi.working_time_in_seconds,
  rider_kpi.planned_working_time_in_seconds,
  rider_kpi.break_time_in_seconds,
  rider_kpi.swaps_accepted_count,
  rider_kpi.swaps_pending_no_show_count,
  rider_kpi.swaps_accepted_no_show_count,
  rider_kpi.created_date_local,
  rider_kpi.contract_start_date_local,
  rider_kpi.contract_end_date_local,
  rider_kpi.contract_creation_date_local,
  rider_kpi.hiring_date_local,
FROM `{project_id}.pandata_intermediate.lg_rider_kpi` AS rider_kpi
LEFT JOIN `{project_id}.pandata_intermediate.pd_countries` AS countries
       ON rider_kpi.country_code = countries.lg_country_code
