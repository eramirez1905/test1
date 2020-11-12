SELECT
  utr_target_periods.uuid,
  countries.rdbms_id,
  utr_target_periods.lg_city_uuid,
  utr_target_periods.lg_city_id,
  utr_target_periods.lg_zone_uuid,
  utr_target_periods.lg_zone_id,
  utr_target_periods.country_code,
  utr_target_periods.period_name,
  utr_target_periods.weekday,
  utr_target_periods.start_time_local,
  utr_target_periods.end_time_local,
  utr_target_periods.timezone,
  utr_target_periods.created_at_utc,
  utr_target_periods.history,
FROM `{project_id}.pandata_intermediate.lg_utr_target_periods` AS utr_target_periods
LEFT JOIN `{project_id}.pandata_intermediate.pd_countries` AS countries
       ON utr_target_periods.country_code = countries.lg_country_code
