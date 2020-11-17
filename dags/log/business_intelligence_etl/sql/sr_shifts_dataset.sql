CREATE OR REPLACE TABLE il.sr_shifts_dataset
PARTITION BY report_date_local AS
WITH dates AS (
  SELECT date_time
  FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(
    TIMESTAMP_TRUNC(TIMESTAMP_SUB(CAST(DATE_ADD('{{ next_ds }}', INTERVAL 2 WEEK) AS TIMESTAMP), INTERVAL 50 DAY), DAY),
    TIMESTAMP_TRUNC(CAST(DATE_ADD('{{ next_ds }}', INTERVAL 2 WEEK) AS TIMESTAMP), HOUR), INTERVAL 15 MINUTE)
  ) AS date_time
), report_dates AS (
  SELECT CAST(date_time AS DATE) AS report_date
    , date_time AS start_datetime
    , TIMESTAMP_ADD(date_time, INTERVAL 15 MINUTE) AS end_datetime
  FROM dates
), shifts AS (
  SELECT *
  FROM il.shifts
  WHERE created_date >= DATE_SUB(DATE_SUB('{{ next_ds }}', INTERVAL 5 WEEK), INTERVAL 1 DAY)
    AND CAST(DATETIME(created_at, timezone) AS DATE) BETWEEN DATE_SUB('{{ next_ds }}', INTERVAL 5 WEEK) AND DATE_ADD('{{ next_ds }}', INTERVAL 2 WEEK)
), absences AS (
  SELECT *
  FROM il.absences
  WHERE DATE(absence_starts_at) >= DATE_SUB('{{ next_ds }}', INTERVAL 6 WEEK)
), evaluations AS (
  SELECT *
  FROM il.evaluations
  WHERE shift_start_date >= DATE_SUB('{{ next_ds }}', INTERVAL 6 WEEK)
), temp_shifts AS (
  SELECT s.country_code
    , s.shift_id
    , s.repetition_number
    , s.courier_id
    , s.repeating
    , s.city_id
    , s.zone_id
    , s.no_utr
    , s.shift_state
    , s.no_show
    , s.tag
    , CAST(s.evaluation_start_time AS DATE) AS evaluation_start_date
    , s.evaluation_start_time
    , s.evaluation_end_time
    , CAST(s.shift_start_time AS DATE) AS shift_start_date
    , s.shift_start_time
    , s.shift_end_time
    , CAST(DATETIME(s.evaluation_start_time, s.timezone) AS DATE) AS evaluation_start_date_local
    , DATETIME(s.evaluation_start_time, s.timezone) AS evaluation_start_time_local
    , DATETIME(s.evaluation_end_time, s.timezone) AS evaluation_end_time_local
    , s.evaluation_duration_min
    , s.evaluation_break_min
    , CAST(DATETIME(s.shift_start_time, s.timezone) AS DATE) AS shift_start_date_local
    , DATETIME(s.shift_start_time, s.timezone) AS shift_start_time_local
    , DATETIME(s.shift_end_time, s.timezone) AS shift_end_time_local
    , s.shift_duration_min
    , s.login_diff_min
    , s.logout_diff_min
    , s.late_logout
    , s.early_logout
    , s.late_login
    , s.early_login
    , s.created_at
    , s.parent_id
    , ra.absence_reason
    , s.timezone
    , CASE
        WHEN shift_state NOT IN ('no_show')
          THEN NULL
        WHEN ra.absence_reason IS NOT NULL AND shift_state IN ('no_show')
         THEN TRUE
        ELSE FALSE
      END AS excused_ns
    , CASE
        WHEN shift_state NOT IN ('no_show')
          THEN NULL
         WHEN ra.absence_reason IS NULL AND shift_state IN ('no_show')
          THEN TRUE
        ELSE FALSE
      END AS unexcused_ns
    , CASE
        WHEN shift_state NOT IN ('no_show')
          THEN NULL
        WHEN ra.absence_reason IS NOT NULL AND shift_state IN ('no_show') and ra.created_at < s.shift_start_time
          THEN TRUE
        ELSE FALSE
      END AS known_ns
  FROM shifts s
  LEFT JOIN absences ra ON s.courier_id = ra.employment_id
    AND s.country_code = ra.country_code
    AND ra.absence_starts_at <= DATETIME(s.shift_end_time, s.timezone)
    AND DATETIME(s.shift_start_time, s.timezone) <= ra.absence_ends_at
), temp_shift_planned_cal AS (
  SELECT DISTINCT s.country_code
    , s.city_id
    , dates.report_date
    , dates.start_datetime
    , dates.end_datetime
    , CAST(DATETIME(dates.start_datetime, s.timezone) AS DATE) AS report_date_local
    , DATETIME(dates.start_datetime, s.timezone) AS start_datetime_local
    , DATETIME(dates.end_datetime, s.timezone) AS end_datetime_local
    , s.courier_id
    , s.shift_id
    , s.tag
    , s.zone_id
    , s.absence_reason
    , s.excused_ns
    , s.known_ns
    , s.shift_state
    , s.no_utr
    , s.repeating
    , s.no_show
    , s.parent_id
    , s.timezone
    , LEAST(dates.end_datetime, shift_end_time) AS least
    , GREATEST(shift_start_time, dates.start_datetime) AS greatest
  FROM temp_shifts s
  INNER JOIN report_dates dates ON s.shift_start_time < dates.end_datetime
    AND dates.start_datetime < s.shift_end_time
  WHERE s.no_utr IS FALSE
), temp_shift_planned AS (
  SELECT *
    , TIMESTAMP_DIFF(least, greatest, SECOND) / 60 AS shift_mins
    , TIMESTAMP_DIFF(least, greatest, SECOND) / 60 /60 AS hours
  FROM temp_shift_planned_cal
), late_logout AS (
  SELECT DISTINCT s.country_code
    , s.city_id
    , dates.report_date
    , TIMESTAMP_SUB(dates.start_datetime, INTERVAL 15 MINUTE) AS start_datetime
    , dates.end_datetime
    , CAST(DATETIME(dates.start_datetime, s.timezone) AS DATE) AS report_date_local
    , DATETIME(dates.start_datetime, s.timezone) AS start_datetime_local
    , DATETIME(dates.end_datetime, s.timezone) AS end_datetime_local
    , s.courier_id
    , s.shift_id
    , s.tag
    , zone_id
    , s.late_logout
    , shift_end_time_local
    , s.evaluation_end_time_local
    , CASE
        WHEN s.late_logout IS TRUE
          AND TIMESTAMP_DIFF(LEAST(dates.end_datetime, s.evaluation_end_time),
            GREATEST(shift_end_time, dates.start_datetime), SECOND) / 60 > 15
          THEN NULL
        WHEN s.late_logout IS TRUE
          THEN TIMESTAMP_DIFF(LEAST(dates.end_datetime, s.evaluation_end_time),
            GREATEST(shift_end_time, dates.start_datetime), SECOND) / 60
      END AS late_logout_hours
  FROM temp_shifts s
  INNER JOIN report_dates dates ON shift_end_time BETWEEN dates.start_datetime AND dates.end_datetime
    AND evaluation_end_time BETWEEN dates.start_datetime AND dates.end_datetime
  WHERE s.no_utr IS FALSE
    AND s.no_show IS FALSE
    AND s.late_logout IS TRUE
), early_login AS (
  SELECT DISTINCT s.country_code
    , s.city_id
    , dates.report_date
    , TIMESTAMP_ADD(dates.start_datetime, INTERVAL 15 MINUTE) AS start_datetime
    , dates.end_datetime
    , CAST(DATETIME(dates.start_datetime, s.timezone) AS DATE) AS report_date_local
    , DATETIME(dates.start_datetime, s.timezone) AS start_datetime_local
    , DATETIME(dates.end_datetime, s.timezone) AS end_datetime_local
    , s.courier_id
    , s.shift_id
    , s.tag
    , zone_id
    , s.early_login
    , CASE
        WHEN s.early_login IS TRUE
          AND TIMESTAMP_DIFF(LEAST(dates.end_datetime, s.shift_start_time),
            GREATEST(evaluation_start_time, dates.start_datetime), SECOND) / 60 > 15
          THEN NULL
        WHEN s.early_login IS TRUE
          THEN TIMESTAMP_DIFF(LEAST(dates.end_datetime, s.shift_start_time),
            GREATEST(evaluation_start_time, dates.start_datetime), SECOND) / 60
      END AS early_login_hours
  FROM temp_shifts s
  INNER JOIN report_dates dates ON s.evaluation_start_time <= dates.end_datetime
    AND dates.start_datetime <= s.shift_end_time
  WHERE s.no_utr IS FALSE
    AND s.no_show IS FALSE
    AND s.early_login IS TRUE
), late_login AS (
  SELECT DISTINCT s.country_code
    , s.city_id
    , dates.report_date
    , dates.start_datetime
    , dates.end_datetime
    , CAST(DATETIME(dates.start_datetime, s.timezone) AS DATE) AS report_date_local
    , DATETIME(dates.start_datetime, s.timezone) AS start_datetime_local
    , DATETIME(dates.end_datetime, s.timezone) AS end_datetime_local
    , s.courier_id
    , s.shift_id
    , s.tag
    , zone_id
    , s.late_login
    , s.evaluation_start_time_local
    , s.shift_start_time_local
    , s.shift_end_time_local
    , s.evaluation_end_time_local
    , CASE
        WHEN s.late_login IS TRUE
          AND TIMESTAMP_DIFF(LEAST(dates.end_datetime, s.evaluation_start_time),
            GREATEST(s.shift_start_time, dates.start_datetime), SECOND) / 60 > 15
          THEN NULL
        WHEN s.late_login IS TRUE
          THEN TIMESTAMP_DIFF(LEAST(dates.end_datetime, s.evaluation_start_time),
            GREATEST(s.shift_start_time, dates.start_datetime), SECOND) / 60
      END AS late_login_hours
  FROM temp_shifts s
  INNER JOIN report_dates dates ON shift_start_time <= dates.end_datetime
    AND dates.start_datetime <= shift_end_time
  WHERE s.no_utr IS FALSE
    AND s.no_show IS FALSE
    AND s.late_login IS TRUE
), temp_shifts_agg AS (
  SELECT DISTINCT s.country_code
    , s.city_id
    , s.report_date_local
    , s.start_datetime_local
    , s.end_datetime_local
    , s.report_date
    , s.start_datetime
    , s.end_datetime
    , s.courier_id
    , s.shift_id
    , s.tag
    , s.zone_id
    , s.shift_mins
    , hours
    , s.shift_state
    , s.timezone
    , ROW_NUMBER () OVER (PARTITION BY s.country_code, s.city_id, s.zone_id, s.report_date, s.start_datetime, s.shift_id ORDER BY s.start_datetime ASC) AS shift_rn
    , el.early_login_hours
    , ll.late_logout_hours
    , li.late_login_hours
    , el.early_login
    , ll.late_logout
    , li.late_login
    , s.excused_ns
    , repeating
    , s.no_show
    , s.parent_id
  FROM temp_shift_planned s
  LEFT JOIN early_login el USING (report_date, start_datetime, shift_id)
  LEFT JOIN late_logout ll USING (report_date, start_datetime, shift_id)
  LEFT JOIN late_login li USING (report_date, start_datetime, shift_id)
), temp_shifts_dataset AS (
  SELECT t.country_code
    , t.city_id
    , t.report_date_local
    , t.start_datetime_local
    , t.end_datetime_local
    , t.zone_id
    , CAST(t.start_datetime_local AS TIME) AS time_local
    , SUM(IF(shift_rn = 1, t.hours, NULL)) AS shift_hours
    , SUM(IF(t.parent_id IS NOT NULL OR t.tag IS NULL AND shift_rn = 1, t.hours, NULL)) AS repeating_shift_hours
    , SUM(IF(t.tag IN ('SWAP') AND t.parent_id IS NULL AND shift_rn = 1, t.hours, NULL)) AS swap_shift_hours
    , SUM(IF(t.tag IN ('MANUAL') AND t.parent_id IS NULL AND shift_rn = 1, t.hours, NULL)) AS manual_shift_hours
    , SUM(IF(t.tag IN ('APPLICATION') AND shift_rn = 1, t.hours, NULL)) AS application_shift_hours
    , SUM(IF(t.tag IN ('HURRIER') AND shift_rn = 1, t.hours, NULL)) AS hurrier_shift_hours
    , SUM(IF(t.tag IN ('AUTOMATIC') AND shift_rn = 1, t.hours, NULL)) AS automatic_shift_hours
    , SUM(IF(t.shift_state NOT IN ('no_show') AND shift_rn = 1, t.hours, NULL)) AS shift_hours_without_ns
    , SUM(IF(t.shift_state IN ('no_show') AND shift_rn = 1, t.hours, NULL)) AS ns_hours
    , SUM(IF(t.shift_state IN ('no_show') AND excused_ns IS TRUE AND shift_rn = 1, t.hours, NULL)) excused_ns_hours
    , SUM(IF(early_login_hours > 0 AND shift_rn = 1, early_login_hours, NULL)) AS early_login_hours
    , SUM(IF(late_logout_hours > 0 AND shift_rn = 1, late_logout_hours, NULL)) AS late_logout_hours
    , SUM(IF(late_login_hours > 0 AND shift_rn = 1, late_login_hours, NULL)) AS late_login_hours
    , COUNT(IF(shift_rn = 1, t.shift_id, NULL)) AS shifts_num
    , COUNT(IF(shift_rn = 1 AND no_show IS FALSE, t.shift_id, NULL)) AS worked_shifts_num
    , COUNT(IF(shift_rn = 1 AND late_logout IS TRUE, t.shift_id, NULL)) AS late_logout_shifts
    , COUNT(IF(shift_rn = 1 AND early_login IS TRUE, t.shift_id, NULL)) AS early_login_shifts
    , COUNT(IF(shift_rn = 1 AND late_login IS TRUE, t.shift_id, NULL)) AS late_login_shifts
    , COUNT(IF(shift_rn = 1 AND t.repeating IS TRUE, t.shift_id, NULL)) AS repeating_shifts
  FROM temp_shifts_agg t
  GROUP BY 1, 2, 3, 4, 5, 6, 7
), temp_evaluations AS (
  SELECT DISTINCT s.country_code
    , s.city_id
    , dates.report_date
    , CAST(DATETIME(dates.start_datetime, s.timezone) AS DATE) AS report_date_local
    , dates.start_datetime
    , dates.end_datetime
    , DATETIME(dates.start_datetime, s.timezone) AS start_datetime_local
    , DATETIME(dates.end_datetime, s.timezone) AS end_datetime_local
    , TIMESTAMP(s.start_time, s.timezone) AS start_time_utc
    , TIMESTAMP(s.end_time, s.timezone) AS end_time_utc
    , s.courier_id
    , s.shift_id
    , s.tag
    , zone_id
    , s.type
    , LEAST(dates.end_datetime, TIMESTAMP(s.end_time, s.timezone)) AS least
    , GREATEST(dates.start_datetime, TIMESTAMP(s.start_time, s.timezone)) AS greatest
  FROM evaluations s
  INNER JOIN report_dates dates ON TIMESTAMP(s.start_time, s.timezone) <= dates.end_datetime
    AND dates.start_datetime <= TIMESTAMP(s.end_time, s.timezone)
  WHERE s.no_utr IS FALSE
), temp_evaluations_calc AS (
  SELECT *
    , TIMESTAMP_DIFF(least, greatest, SECOND) /60 AS evaluation_mins
    , TIMESTAMP_DIFF(least, greatest, SECOND) /60 /60 AS hours
  FROM temp_evaluations
), temp_evaluations_dataset AS (
  SELECT e.country_code
    , e.city_id
    , e.report_date_local
    , e.start_datetime_local
    , e.end_datetime_local
    , zone_id
    , SUM(IF(e.type IN ('working'), e.hours, NULL)) AS evaluations_working_hours
    , SUM(IF(e.type NOT IN ('working'), e.hours, NULL)) AS evaluations_break_hours
  FROM temp_evaluations_calc e
  GROUP BY 1,2,3,4,5,6
)
SELECT t.country_code
    , t.city_id
    , t.report_date_local
    , t.start_datetime_local
    , t.end_datetime_local
    , t.zone_id
    , t.time_local
    , SUM(t.shift_hours) AS shift_hours
    , SUM(e.evaluations_working_hours) AS evaluations_working_hours
    , SUM(e.evaluations_break_hours) AS evaluations_break_hours
    , SUM(t.repeating_shift_hours) AS repeating_shift_hours
    , SUM(t.swap_shift_hours) AS swap_shift_hours
    , SUM(t.manual_shift_hours) AS manual_shift_hours
    , SUM(t.application_shift_hours) AS application_shift_hours
    , SUM(t.hurrier_shift_hours) AS hurrier_shift_hours
    , SUM(t.automatic_shift_hours) AS automatic_shift_hours
    , SUM(t.shift_hours_without_ns) AS shift_hours_without_ns
    , SUM(t.ns_hours) AS ns_hours
    , SUM(t.excused_ns_hours) excused_ns_hours
    , SUM(t.early_login_hours) AS early_login_hours
    , SUM(t.late_logout_hours) AS late_logout_hours
    , SUM(t.late_login_hours) AS late_login_hours
    , SUM(t.shifts_num) AS shifts_num
    , SUM(t.worked_shifts_num) AS worked_shifts_num
    , SUM(t.late_logout_shifts) AS late_logout_shifts
    , SUM(t.early_login_shifts) AS early_login_shifts
    , SUM(t.late_login_shifts) AS late_login_shifts
    , SUM(t.repeating_shifts) AS repeating_shifts
  FROM temp_shifts_dataset t
  LEFT JOIN temp_evaluations_dataset e ON e.country_code = t.country_code
    AND e.report_date_local = t.report_date_local
    AND e.start_datetime_local = t.start_datetime_local
    AND e.city_id = t.city_id
    AND e.zone_id = t.zone_id
  GROUP BY 1, 2, 3, 4, 5, 6, 7
;
