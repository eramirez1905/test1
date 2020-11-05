CREATE OR REPLACE TABLE il.staffing_report
PARTITION BY report_date AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 5 WEEK) AS start_date
    , DATE_ADD('{{ next_ds }}', INTERVAL 2 WEEK) AS end_date
), dates AS (
  SELECT date_time
  FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(
    TIMESTAMP_TRUNC(TIMESTAMP_SUB((SELECT CAST(end_date AS TIMESTAMP) FROM parameters), INTERVAL 50 DAY), DAY),
    TIMESTAMP_TRUNC((SELECT CAST(end_date AS TIMESTAMP) FROM parameters), HOUR), INTERVAL 15 MINUTE)
  ) AS date_time
), report_dates AS (
  SELECT CAST(date_time AS DATE) AS report_date
    , date_time AS start_datetime
    , TIMESTAMP_ADD(date_time, INTERVAL 15 MINUTE) AS end_datetime
    , LAG(CAST(date_time AS DATE),21) OVER(ORDER BY date_time ASC) AS working_day
  FROM dates
), countries AS (
  SELECT country_code
    , ci.id AS city_id
    , ci.name AS city_name
    , ci.timezone AS timezone
    , zo.id AS zone_id
    , zo.geo_id
    , zo.name AS zone_name
  FROM `{{ params.project_id }}.cl.countries` co
  LEFT JOIN UNNEST(co.cities) ci
  LEFT JOIN UNNEST(ci.zones) zo
), temp_rider_hours_dataset AS (
  SELECT COALESCE(t.country_code, op.country_code) AS country_code
   , COALESCE(t.city_id, op.city_id) AS city_id
   , COALESCE(t.report_date_local, op.report_date_local) AS report_date_local
   , COALESCE(t.start_datetime_local, op.start_datetime_local) AS start_datetime_local
   , COALESCE(t.zone_id, op.zone_id) AS zone_id
   , CAST(CAST(t.start_datetime_local AS TIME) AS STRING) AS time_local
   , t.end_datetime_local
   , op.timezone
   , SUM(t.shift_hours) AS shift_hours
   , SUM(t.swap_shift_hours) AS swap_shift_hours
   , SUM(t.manual_shift_hours) AS manual_shift_hours
   , SUM(t.application_shift_hours) AS application_shift_hours
   , SUM(t.hurrier_shift_hours) AS hurrier_shift_hours
   , SUM(t.automatic_shift_hours) AS automatic_shift_hours
   , SUM(t.shift_hours_without_ns) AS shift_hours_without_ns
   , SUM(t.ns_hours) AS ns_hours
   , SUM(t.excused_ns_hours) AS excused_ns_hours
   , SUM(t.evaluations_working_hours) AS evaluations_working_hours
   , SUM(t.evaluations_break_hours) AS evaluations_break_hours
   , SUM(shifts_num) AS shifts_num
   , SUM(worked_shifts_num) AS worked_shifts_num
   , SUM(early_login_hours) AS early_login_hours
   , SUM(late_logout_hours) AS late_logout_hours
   , SUM(late_login_hours) AS late_login_hours
   , SUM(late_logout_shifts) AS late_logout_shifts
   , SUM(early_login_shifts) AS early_login_shifts
   , SUM(late_login_shifts) AS late_login_shifts
   , SUM(repeating_shift_hours) AS repeating_shift_hours
   , SUM(repeating_shifts) AS repeating_shifts
   , SUM(open_slots_size) AS open_slots_size
   , SUM(open_slots_unassigned) AS open_slots_unassigned
   , SUM(open_slots_assigned) AS open_slots_assigned
   , SUM(swap_open_slots) AS swap_open_slots
   , SUM(application_slots_size) AS application_slots_size
   , SUM(hurrier_slots_size) AS hurrier_slots_size
   , SUM(automatic_slots_size) AS automatic_slots_size
   , SUM(repeating_slots_size) AS repeating_slots_size
   , SUM(manual_slots_size) AS manual_slots_size
   , SUM(trans_working) AS trans_working
   , SUM(trans_break) AS trans_break
   , SUM(trans_temp_not_working) AS trans_temp_not_working
   , SUM(trans_busy_time) AS trans_busy_time
  FROM il.sr_shifts_dataset t
  FULL JOIN il.sr_open_slots_dataset op ON op.country_code = t.country_code
    AND op.report_date_local = t.report_date_local
    AND op.start_datetime_local = t.start_datetime_local
    AND op.city_id = t.city_id
    AND op.zone_id = t.zone_id
  LEFT JOIN il.sr_transitions_dataset tr ON tr.country_code = t.country_code
    AND tr.report_date_local = t.report_date_local
    AND tr.start_datetime_local = t.start_datetime_local
    AND tr.city_id = t.city_id
    AND tr.zone_id = t.zone_id
--  WHERE COALESCE(t.country_code, op.country_code) = ANY((SELECT countries FROM parameters)::TEXT[])
  GROUP BY 1,2,3,4,5,6,7,8
), temp_rider_orders_dataset AS (
  SELECT COALESCE(d.country_code, f.country_code, o.country_code) AS country_code
    , COALESCE(d.city_id, f.city_id, o.city_id) AS city_id
    , COALESCE(d.report_date_local, f.report_date_local, o.report_date_local) AS report_date_local
    , COALESCE(d.start_datetime_local, f.start_datetime_local, o.start_datetime_local) AS start_datetime_local
    , COALESCE(d.zone_id, f.zone_id, o.zone_id) AS zone_id
    , CAST(CAST(COALESCE(d.start_datetime_local, f.start_datetime_local, o.start_datetime_local) AS TIME) AS STRING) AS time_local
    , d.end_datetime_local
    , d.timezone
    , SUM(deliveries) AS deliveries
    , SUM(d.orders_delivered) AS orders_delivered
    , SUM(promised_dt_n) AS promised_dt_n
    , SUM(promised_dt_d) AS promised_dt_d
    , SUM(delivery_time_n) AS delivery_time_n
    , SUM(delivery_time_d) AS delivery_time_d
    , SUM(NULL) AS exp_orders_d7
    , SUM(exp_orders_latest) AS exp_orders_latest
    , SUM(NULL) AS exp_riders_d7
    , SUM(exp_riders_latest) AS exp_riders_latest
    , SUM(COALESCE(exp_no_shows_latest, 0.0)) AS exp_no_shows_latest
    , SUM(NULL) AS exp_no_shows_d7
    , SUM(original_orders) AS original_orders
    , SUM(orders_actuals) AS orders_actuals
    , SUM(delay_in_mins) AS delay_in_mins
    , SUM(delivery_delay_10_count) AS delivery_delay_10_count
    , SUM(delivery_delay_count) AS delivery_delay_count
  FROM il.sr_deliveries_over_interval d
  FULL JOIN il.sr_orders_over_interval o ON o.country_code = d.country_code
    AND o.report_date_local = d.report_date_local
    AND o.start_datetime_local = d.start_datetime_local
    AND o.city_id = d.city_id
    AND o.zone_id = d.zone_id
  FULL JOIN `{{ params.project_id }}.cl._sr_latest_forecast_dataset` f ON f.country_code = d.country_code
    AND f.report_date_local = d.report_date_local
    AND f.start_datetime_local = d.start_datetime_local
    AND f.city_id = d.city_id
    AND f.zone_id = d.zone_id
  --WHERE COALESCE(d.country_code, f.country_code, o.country_code) = ANY((SELECT countries FROM parameters)::TEXT[])
  GROUP BY 1,2,3,4,5,6,7,8
), utr_target_periods AS (
  SELECT tp.country_code
    , z.geo_id
    , z.city_id
    , tp.zone_external_id AS zone_id
    , weekday
    , tp.to
    , tp.from AS starts_at
    , CASE
        WHEN tp.to = '00:00:00'
          THEN '23:59:59'
        ELSE tp.to
      END AS ends_at
    , tp.name AS time_period
    , utr
  FROM `{{ params.project_id }}.ml.forecast_utr_target_periods` AS tp
  LEFT JOIN countries AS z ON z.country_code = tp.country_code
    AND z.zone_id = tp.zone_external_id
), final AS (
  SELECT COALESCE(t.country_code, o.country_code) AS country_code
     , COALESCE(t.city_id, o.city_id) AS city_id
     , COALESCE(t.report_date_local, o.report_date_local) AS report_date
     , COALESCE(t.start_datetime_local, o.start_datetime_local) AS start_datetime
     , COALESCE(t.zone_id, o.zone_id) AS zone_id
     , COALESCE(t.time_local, o.time_local) AS time
     , COALESCE(t.end_datetime_local, o.end_datetime_local) AS end_datetime
     , COALESCE(t.timezone, o.timezone) AS timezone
     , SUM(t.shift_hours) AS shift_hours
     , SUM(t.swap_shift_hours) AS swap_shift_hours
     , SUM(t.manual_shift_hours) AS manual_shift_hours
     , SUM(t.application_shift_hours) AS application_shift_hours
     , SUM(t.hurrier_shift_hours) AS hurrier_shift_hours
     , SUM(t.automatic_shift_hours) AS automatic_shift_hours
     , SUM(t.shift_hours_without_ns) AS shift_hours_without_ns
     , SUM(t.ns_hours) AS ns_hours
     , SUM(t.excused_ns_hours) AS excused_ns_hours
     , SUM(t.evaluations_working_hours) AS evaluations_working_hours
     , SUM(t.evaluations_break_hours) AS evaluations_break_hours
     , SUM(t.shifts_num) AS shifts_num
     , SUM(t.worked_shifts_num) AS worked_shifts_num
     , SUM(orders_delivered) AS orders_delivered
     , SUM(orders_actuals) AS orders_actuals
     , SUM(deliveries) AS deliveries
     , SUM(promised_dt_n) AS promised_dt_n
     , SUM(promised_dt_d) AS promised_dt_d
     , SUM(delivery_time_n) AS delivery_time_n
     , SUM(delivery_time_d) AS delivery_time_d
     , SUM(CAST(exp_orders_d7 AS NUMERIC) / 2) AS exp_orders_d7
     , SUM(CAST(exp_orders_latest AS NUMERIC) / 2) AS exp_orders_latest
     , SUM(CAST(exp_riders_d7 AS NUMERIC) / 2) AS exp_riders_d7
     , SUM(CAST(exp_riders_latest AS NUMERIC) / 2) AS exp_riders_latest
     , SUM(CAST(exp_no_shows_latest AS NUMERIC) / 2) AS exp_no_shows_latest
     , SUM(CAST(exp_no_shows_d7 AS NUMERIC) / 2) AS exp_no_shows_d7
     , SUM(CAST(original_orders AS NUMERIC) / 2) AS original_orders
     , SUM (repeating_shift_hours) AS repeating_shift_hours
     , SUM(repeating_shifts) AS repeating_shifts
     , SUM(open_slots_size) AS open_slots_size
     , SUM(open_slots_assigned) AS open_slots_assigned
     , SUM(open_slots_unassigned) AS open_slots_unassigned
     , SUM(swap_open_slots) AS swap_open_slots
     , SUM(application_slots_size) AS application_slots_size
     , SUM(hurrier_slots_size) AS hurrier_slots_size
     , SUM(automatic_slots_size) AS automatic_slots_size
     , SUM(repeating_slots_size) AS repeating_slots_size
     , SUM(manual_slots_size) AS manual_slots_size
     , SUM(trans_working) AS trans_working
     , SUM(trans_break) AS trans_break
     , SUM(trans_temp_not_working) AS trans_temp_not_working
     , COALESCE(SUM(trans_temp_not_working),0) + COALESCE(SUM(trans_working),0) AS trans_net_working
     , SUM(early_login_hours) AS early_login_hours
     , SUM(late_logout_hours) AS late_logout_hours
     , SUM(late_login_hours) AS late_login_hours
     , SUM(late_logout_shifts) AS late_logout_shifts
     , SUM(early_login_shifts) AS early_login_shifts
     , SUM(late_login_shifts) AS late_login_shifts
     , SUM(trans_busy_time) AS trans_busy_time
     , SUM(delay_in_mins) AS delay_in_mins
     , SUM(o.delivery_delay_10_count) AS delivery_delay_10_count
     , SUM(o.delivery_delay_count) AS delivery_delay_count
  FROM temp_rider_hours_dataset t
  FULL JOIN temp_rider_orders_dataset o ON t.country_code = o.country_code
    AND t.report_date_local = o.report_date_local
    AND t.start_datetime_local = o.start_datetime_local
    AND t.city_id = o.city_id
    AND t.zone_id = o.zone_id
  GROUP BY 1,2,3,4,5,6,7,8
)
 SELECT f.country_code
    , f.report_date
    , co.country_name
    , co.venture_name
    , FORMAT_DATE("%G-%V", f.report_date) report_week
    , FORMAT_DATE("%Y-%m", f.report_date) AS report_month
    , f.city_id
    , f.zone_id
    , f.start_datetime
    , f.time
    , f.end_datetime
    , f.shift_hours
    , f.swap_shift_hours
    , f.manual_shift_hours
    , f.application_shift_hours
    , f.hurrier_shift_hours
    , f.automatic_shift_hours
    , f.shift_hours_without_ns
    , f.ns_hours
    , f.excused_ns_hours
    , f.evaluations_working_hours
    , f.evaluations_break_hours
    , f.shifts_num
    , f.worked_shifts_num
    , f.orders_delivered
    , f.orders_actuals
    , f.deliveries
    , f.promised_dt_n
    , f.promised_dt_d
    , f.delivery_time_n
    , f.delivery_time_d
    , f.exp_orders_d7
    , f.exp_orders_latest
    , f.exp_riders_d7
    , f.exp_riders_latest
    , f.exp_no_shows_latest
    , f.exp_no_shows_d7
    , f.original_orders
    , f.repeating_shift_hours
    , f.repeating_shifts
    , f.open_slots_size
    , f.open_slots_assigned
    , f.open_slots_unassigned
    , f.manual_slots_size
    , f.swap_open_slots
    , f.application_slots_size
    , f.hurrier_slots_size
    , f.automatic_slots_size
    , f.repeating_slots_size
    , f.trans_working
    , f.trans_break
    , f.trans_temp_not_working
    , f.trans_net_working
    , f.early_login_hours
    , f.late_logout_hours
    , f.late_login_hours
    , f.late_logout_shifts
    , f.early_login_shifts
    , f.late_login_shifts
    , f.trans_busy_time
    , f.delay_in_mins
    , f.delivery_delay_10_count
    , f.delivery_delay_count
    , working_day
    , FORMAT_DATE("%A", working_day) AS report_weekday
    , FORMAT_DATE("%B", working_day) AS report_month_name
    , FORMAT_DATE("%Y-%m", working_day) AS report_month_wd
    , FORMAT_DATE("%G-%V", working_day) report_week_wd
    , ci.name AS city_name
    , zo.name AS zone_name
    , tp.starts_at AS utr_target_period_start
    , tp.ends_at AS utr_target_period_end
    , tp.time_period AS utr_target_period_name
    , tp.utr AS target_utr
    , lo.close_orders_lost_net
    , lo.shrink_orders_lost_net
  FROM report_dates dates
  LEFT JOIN final f ON f.report_date = dates.report_date
    AND CAST(dates.start_datetime AS DATETIME) = f.start_datetime
  LEFT JOIN cl.countries c ON f.country_code = c.country_code
  LEFT JOIN UNNEST(c.cities) ci ON f.city_id = ci.id
  LEFT JOIN UNNEST(ci.zones) zo ON zo.id = f.zone_id
  LEFT JOIN utr_target_periods tp ON f.country_code = tp.country_code
    AND f.city_id = tp.city_id
    AND f.zone_id = tp.zone_id
    AND LOWER(REPLACE(FORMAT_DATE("%A", f.report_date), ' ', '')) = LOWER(REPLACE(tp.weekday, ' ', ''))
    AND CAST(f.start_datetime AS TIME) < tp.ends_at
    AND tp.starts_at < CAST(f.end_datetime AS TIME)
  LEFT JOIN il.sr_lost_orders lo ON f.country_code = lo.country_code
    AND f.city_id = lo.city_id
    AND f.zone_id = lo.zone_id
    AND f.report_date = lo.report_date_local
    AND f.start_datetime = lo.start_datetime_local
  LEFT JOIN il.countries co ON f.country_code = co.country_code
;
