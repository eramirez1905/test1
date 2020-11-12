CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.compliance_report`
PARTITION BY processed_date AS
WITH shifts AS (
  SELECT country_code
    , a.violation_id
    , rider_id
    , COUNT(IF(created_date < '{{ next_ds }}', shift_id, NULL)) AS past_shifts
    , COUNT(IF(created_date > '{{ next_ds }}', shift_id, NULL)) AS future_shifts
  FROM `{{ params.project_id }}.cl.shifts`
  LEFT JOIN UNNEST (absences) a
  GROUP BY 1, 2, 3
), batches AS (
  SELECT country_code
    , rider_id
    , b.number AS batch_number
    , b.active_from
    , b.active_until
  FROM `{{ params.project_id }}.cl.riders` r
  LEFT JOIN UNNEST (batches) b
), active_riders AS (
  SELECT s.country_code
  , b.batch_number
  , FORMAT_DATE("%G-%V", report_date) AS report_week
  , COUNT(DISTINCT(IF(report_date BETWEEN DATE(s.actual_start_at, s.timezone) AND DATE(s.actual_end_at, s.timezone), s.rider_id, NULL))) AS active_riders
FROM `{{ params.project_id }}.cl.shifts` s
LEFT JOIN batches b ON s.country_code = b.country_code
  AND s.rider_id = b.rider_id
  AND s.actual_start_at BETWEEN b.active_from AND b.active_until
  LEFT JOIN UNNEST(GENERATE_DATE_ARRAY(DATE_SUB('{{ next_ds }}', INTERVAL 8 WEEK), '{{ next_ds }}', INTERVAL 1 DAY)) report_date
GROUP BY 1, 2, 3
)
SELECT co.region
  , c.country_code
  , co.country_name
  , DATE(v.processed_at, c.timezone) AS processed_date
  , FORMAT_DATE("%G-%V", DATE(v.processed_at, c.timezone)) AS processed_week
  , r.violation_type AS violation_type
  , r.rule_type As rule_type
  , r.contract_type AS contract_type
  , c.rider_id
  , b.batch_number
  , ar.active_riders
  , COUNTIF(a.type = 'NOTIFICATION' AND a.state IN ('ENDED')) AS notifications
  , COUNTIF(a.type = 'SUSPENSION' AND a.state IN ('ENDED', 'INTERRUPTED', 'STARTED')) AS suspensions
  , SUM(past_shifts) AS shifts_missed
  , SUM(future_shifts) AS upcoming_shifts
  , SUM(IF(a.type = 'SUSPENSION', TIMESTAMP_DIFF(COALESCE(a.ended_at, a.interrupted_at), a.started_at, HOUR), NULL)) AS time_to_lift_suspension_sum
  , COUNT(IF(a.type = 'SUSPENSION', TIMESTAMP_DIFF(COALESCE(a.ended_at, a.interrupted_at), a.started_at, HOUR), NULL)) AS time_to_lift_suspension_count
FROM `{{ params.project_id }}.cl.rider_compliance` c
LEFT JOIN UNNEST (violations) v
LEFT JOIN UNNEST (v.actions) a
LEFT JOIN UNNEST (v.rules) r
LEFT JOIN shifts s ON c.country_code = s.country_code
  AND c.rider_id = s.rider_id
  AND v.id = s.violation_id
  AND a.type = 'SUSPENSION'
LEFT JOIN `{{ params.project_id }}.cl.countries` co ON c.country_code = co.country_code
LEFT JOIN batches b ON c.rider_id = b.rider_id
  AND v.processed_at BETWEEN b.active_from AND b.active_until
  AND co.country_code = b.country_code
LEFT JOIN active_riders ar ON ar.country_code = c.country_code
  AND (ar.batch_number = b.batch_number OR (ar.batch_number IS NULL AND b.batch_number IS NULL))
  AND ar.report_week = FORMAT_DATE("%G-%V", DATE(v.processed_at, c.timezone))
WHERE v.state = 'PROCESSED'
  AND DATE(v.processed_at, c.timezone) >= DATE_SUB('{{ next_ds }}', INTERVAL 8 WEEK)
-- removing darkstore pickers to not impact utr as they do not have orders, only working hours
  AND c.country_code NOT LIKE '%dp%'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
