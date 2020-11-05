SELECT
  id,
  date,
  date_nodash_string,
  iso_day_of_week_int,
  weekday_name,
  iso_week_int,
  iso_year_week_string,
  day_of_month_int,
  month_int,
  full_month_name,
  year_month_string,
  year_int,
FROM `{project_id}.pandata_intermediate.pd_dates`
