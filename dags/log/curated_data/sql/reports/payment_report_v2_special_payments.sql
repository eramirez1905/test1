CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.payment_report_v2_special_payments`
PARTITION BY report_date_local AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 8 WEEK) AS start_date
), cities AS (
  SELECT r.country_code
    , r.rider_id
    , (SELECT city_id FROM UNNEST(contracts) WHERE status = 'VALID' AND CAST(start_at AS DATE) <= '{{ next_ds }}' ORDER BY end_at DESC LIMIT 1) AS city_id
  FROM `{{ params.project_id }}.cl.riders` r
), special_payments AS (
  SELECT *
  FROM `{{ params.project_id }}.cl.rider_special_payments` p
  WHERE p.country_code NOT LIKE '%dp%'
), batches AS (
  SELECT country_code
    , rider_id
    , b.number AS batch_number
    , b.active_from
    , b.active_until
  FROM `{{ params.project_id }}.cl.riders` r
  LEFT JOIN UNNEST(batches) b
), payment_cycles AS (
  SELECT p.region
    , p.country_code
    , p.rider_id
    , DATETIME(b.payment_cycle_start_date, p.timezone) AS payment_cycle_start_date
    , DATETIME(b.payment_cycle_end_date, p.timezone) AS payment_cycle_end_date
    , ARRAY_AGG(DISTINCT(DATE(b.created_at, p.timezone))) AS rider_activity_dates
    , COUNT(DISTINCT(DATE(b.created_at, p.timezone))) AS days_worked
  FROM `{{ params.project_id }}.cl.rider_payments` p
  LEFT JOIN UNNEST (payment_details.basic) b ON b.status IN ('PENDING', 'PAID')
  WHERE b.payment_cycle_start_date IS NOT NULL
    AND b.payment_cycle_end_date IS NOT NULL
    AND p.country_code NOT LIKE '%dp%'
  GROUP BY 1, 2, 3, 4, 5
), days_worked AS (
  SELECT DISTINCT country_code
    , rider_id
    , DATE(s.actual_start_at, s.timezone) AS working_day
  FROM `{{ params.project_id }}.cl.shifts` s
  WHERE s.shift_state = 'EVALUATED'
    AND s.country_code NOT LIKE '%dp%'
  GROUP BY 1, 2, 3
), union_sources AS (
  SELECT d AS payment_date
    , p.country_code
    , p.rider_id
    , sp.total_payment_date
    , sp.created_date AS original_created_date
    , de.payment_id
    , ba.batch_number
    , 'cycle' AS _source
    , SAFE_DIVIDE(COALESCE(de.total, 0), ARRAY_LENGTH(rider_activity_dates)) AS special_payment_total
  FROM special_payments sp
  LEFT JOIN UNNEST(payment_details) de ON de.status IN ('PENDING', 'PAID')
  LEFT JOIN batches ba ON sp.country_code = ba.country_code
    AND ba.rider_id = sp.rider_id
    AND de.created_at BETWEEN ba.active_from AND ba.active_until
  LEFT JOIN payment_cycles p ON sp.country_code = p.country_code
    AND sp.created_date BETWEEN p.payment_cycle_start_date AND p.payment_cycle_end_date
    AND sp.rider_id = p.rider_id
  LEFT JOIN UNNEST(rider_activity_dates) d
  WHERE p.country_code IS NOT NULL

  UNION ALL

  SELECT d.working_day AS payment_date
    , d.country_code
    , d.rider_id
    , sp.total_payment_date
    , sp.created_date AS original_created_date
    , de.payment_id
    , ba.batch_number
    , 'no_cycle' AS _source
    , SAFE_DIVIDE(COALESCE(de.total, 0), COUNT(working_day) OVER (PARTITION BY d.country_code, d.rider_id, sp.created_date)) AS special_payment_total
  FROM days_worked d
  LEFT JOIN  special_payments sp ON sp.country_code = d.country_code
    AND d.rider_id = sp.rider_id
  LEFT JOIN UNNEST(payment_details) de ON de.status IN ('PENDING', 'PAID')
  LEFT JOIN batches ba ON sp.country_code = ba.country_code
    AND ba.rider_id = sp.rider_id
    AND de.created_at BETWEEN ba.active_from AND ba.active_until
  WHERE sp.created_date >= working_day
    AND sp.created_date <= DATE_ADD(working_day, INTERVAL 7 DAY)

  UNION ALL

  SELECT sp.created_date AS payment_date
    , sp.country_code
    , sp.rider_id
    , sp.total_payment_date
    , sp.created_date AS original_created_date
    , de.payment_id
    , ba.batch_number
    , 'no_cycle_no_shift' AS _source
    , COALESCE(de.total, 0) AS special_payment_total
  FROM special_payments sp
  LEFT JOIN UNNEST(payment_details) de ON de.status IN ('PENDING', 'PAID')
  LEFT JOIN batches ba ON sp.country_code = ba.country_code
    AND ba.rider_id = sp.rider_id
    AND de.created_at BETWEEN ba.active_from AND ba.active_until
), valid_dataset AS (
  SELECT country_code
    , city_id
    , payment_date
    , rider_id
    , _source
    , payment_id
    , batch_number
    , _source = FIRST_VALUE(_source) OVER (PARTITION BY country_code, payment_id, rider_id ORDER BY _source ASC) AS is_valid
    , SUM(special_payment_total) AS special_payment_total
  FROM union_sources u
  LEFT JOIN cities c USING (country_code, rider_id)
  GROUP BY 1, 2, 3, 4, 5, 6, 7
)
SELECT country_code
  , city_id
  , payment_date AS report_date_local
  , rider_id
  , batch_number
  , MAX(special_payment_total) AS special_payment_total
FROM valid_dataset
WHERE is_valid
GROUP BY 1, 2, 3, 4, 5
