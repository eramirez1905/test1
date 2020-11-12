CREATE OR REPLACE TABLE il.rider_payroll_report_shifts
PARTITION BY shift_date AS
WITH shifts_ordered AS (
 SELECT co.country_code
   , co.country_name AS country
   , c.city_name
   , FORMAT_DATE("%Y-%m", working_day) AS report_month
   , FORMAT_DATE("%G-%V", CAST(s.schedule_start_time_local AS DATE)) AS report_week
   , s.schedule_start_time_local AS shift_start
   , s.schedule_end_time_local AS shift_end
   , CAST(s.schedule_start_time_local AS DATE) AS shift_date
   , s.planned_schedule_start_time_local AS planned_shift_start
   , s.planned_schedule_end_time_local AS planned_shift_end
   , FORMAT_DATE("%A", CAST(s.schedule_start_time_local AS DATE)) AS day_of_the_week
   , COALESCE(CONCAT(co.country_code, '-', CAST(s.rider_id AS STRING)), 'N/A') AS rider_uuid
   , COALESCE(CAST(s.rider_id AS STRING), 'N/A') AS rider_id
   , COALESCE(e.name, r.first_name, 'deleted') AS rider_name
   , COALESCE(e.email, r.email_address, '?????') AS shyftplan_email
   , CONCAT(c.city_name, ' - ', z.zone_name) AS zone_name
   , 0 AS total_payment
   , NULL AS position
   , ROUND(CAST(COALESCE(s.schedule_duration_mins / 60, 0) AS NUMERIC), 2) AS hours
   , 0 AS scheduler_token
   , s.shift_id
   , ROW_NUMBER() OVER (PARTITION BY s.country_code, s.shift_id, s.rider_id, s.schedule_start_time_local ORDER BY schedule_start_time_local DESC) AS _row_number
   , CAST(s.schedule_end_time_local AS DATE) AS shift_end_date
   , CASE WHEN s.shift_state IN ('no_show') OR (LOWER(z.zone_name) LIKE '%utr%')
      THEN COALESCE(absence_reason, 'Unexcused') ELSE NULL END AS absence_reason
   , s.shift_state
   , CAST(COALESCE(planned_schedule_duration_mins / 60, 0) AS NUMERIC) AS planned_hours
   , cc.name AS contract_type
 FROM il.schedules s
 LEFT JOIN il.absences ra ON CAST(s.planned_schedule_start_time_local AS DATE) = CAST(ra.absence_starts_at AS DATE)
   AND s.rider_id = ra.rider_id
   AND s.country_code = ra.country_code
   AND absence_starts_at < s.planned_schedule_end_time_local
   AND s.planned_schedule_start_time_local < absence_ends_at
 LEFT JOIN il.zones z ON s.country_code = z.country_code
   AND s.zone_id = z.zone_id
 LEFT JOIN il.riders r ON r.country_code = s.country_code
   AND r.rider_id = s.rider_id
 LEFT JOIN il.countries co ON s.country_code = co.country_code
 LEFT JOIN il.cities c ON s.city_id = c.city_id
   AND s.country_code = c.country_code
 LEFT JOIN `{{ params.project_id }}.ml.rooster_employee` e ON r.email_address = e.email
 LEFT JOIN `{{ params.project_id }}.ml.rooster_employee_contract` ec ON ec.country_code = e.country_code
   AND ec.employee_id = e.id
   AND s.schedule_start_time_local < DATETIME(ec.end_at, s.timezone)
   AND DATETIME(ec.start_at, s.timezone) < s.schedule_end_time_local
   AND ec.status IN ('VALID')
   AND ec.created_date IS NOT NULL                 
 LEFT JOIN `{{ params.project_id }}.ml.rooster_contract` cc ON cc.country_code = ec.country_code
   AND ec.contract_id = cc.id
)
SELECT * EXCEPT (_row_number)
FROM shifts_ordered
WHERE _row_number = 1
;
