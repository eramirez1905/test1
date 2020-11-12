CREATE OR REPLACE VIEW `{{  params.project_id }}.{{ params.braze_dataset }}.{{ params.braze_riders_view }}` AS
WITH riders_contracts AS (
  SELECT country_code
    , rider_id
    , email
    , phone_number
    , created_at
    , rider_name
    , custom_fields
    , (SELECT c.type
       FROM UNNEST(contracts) c
       WHERE c.status = 'VALID'
         AND c.start_at <= '{{ ds }}'
       ORDER BY c.start_at DESC LIMIT 1) AS contract_type
  FROM `{{  params.project_id }}.cl.riders`
), riders_shifts AS (
  SELECT r.country_code
    , r.rider_id
    , r.email
    , r.phone_number
    , r.created_at
    , r.rider_name
    , r.contract_type
    , ARRAY_AGG(
        STRUCT(s.shift_start_at
          , s.shift_state
          , s.actual_working_time
        )
    ) AS shifts
  FROM riders_contracts r
  LEFT JOIN `{{  params.project_id }}.cl.shifts` s ON r.country_code = s.country_code
    AND r.rider_id = s.rider_id
  GROUP BY 1,2,3,4,5,6,7
), riders AS (
  SELECT country_code
    , rider_id
    , email
    , phone_number
    , created_at
    , rider_name
    , contract_type
    , (SELECT COUNT(shift_state) > 0 FROM rs.shifts) AS is_first_shift_scheduled
    , (SELECT MIN(shift_start_at) FROM rs.shifts WHERE shift_state = 'EVALUATED') AS first_shift_date
    , (SELECT COUNTIF(shift_state = 'EVALUATED') FROM rs.shifts) AS total_shifts
    , (SELECT ROUND(SUM(actual_working_time) / 3600, 2) FROM rs.shifts WHERE shift_state = 'EVALUATED') AS hours_worked
    , shifts -- For debug
  FROM riders_shifts rs
), rider_wallet AS (
  SELECT country_code
    , rider_id
    , MIN(created_at) AS first_cod_date
  FROM `{{ params.project_id }}.cl.rider_wallet`
  GROUP BY 1,2
), rooster_employee_referral AS (
  SELECT r.rider_id
    , r.country_code
    , c.value
  FROM riders_contracts r
  LEFT JOIN UNNEST(r.custom_fields) c
  WHERE c.name = "referral_url"
), cities AS (
  SELECT country_code
    , c.id
    , c.name AS city_name
  FROM `{{ params.project_id }}.cl.countries` co
  LEFT JOIN UNNEST(co.cities) c
), applicants AS (
  SELECT country_code
    , applicant_id
    , rider_id
    , language_id
    , referred_by_id
  FROM `{{  params.project_id }}.cl.applicants`
  WHERE rider_id IS NOT NULL
), orders_dataset AS (
  SELECT o.country_code
    , d.rider_id
    , ARRAY_AGG(
        STRUCT(d.city_id
          , d.delivery_status
          , d.vehicle.name AS vehicle_name
          , d.created_at
        )
      ) AS rider_orders
  FROM `{{ params.project_id }}.cl.orders` o
  LEFT JOIN UNNEST(o.deliveries) d
  GROUP BY 1,2
), orders AS (
  SELECT country_code
    , rider_id
    , (SELECT city_id FROM od.rider_orders WHERE city_id IS NOT NULL ORDER BY created_at DESC LIMIT 1) AS city_id
    , (SELECT vehicle_name FROM od.rider_orders WHERE vehicle_name IS NOT NULL ORDER BY created_at DESC LIMIT 1) AS vehicle_name
    , (SELECT COUNT(*) FROM od.rider_orders WHERE delivery_status = 'completed') AS total_orders_delivered
    , rider_orders -- For debug
  FROM orders_dataset od
), quest AS (
  SELECT p.rider_id
    , p.country_code
    , MIN(q.payment_cycle_end_date) AS first_payroll_date
  FROM `{{ params.project_id }}.cl.rider_payments` p
  LEFT JOIN UNNEST(p.payment_details.quest) q
  WHERE q.created_at BETWEEN q.payment_cycle_start_date AND q.payment_cycle_end_date
    AND q.status NOT IN ('UNCERTAIN')
  GROUP BY 1,2
), basic AS (
  SELECT p.rider_id
    , p.country_code
    , MIN(b.payment_cycle_end_date) AS first_payroll_date
  FROM `{{ params.project_id }}.cl.rider_payments` p
  LEFT JOIN UNNEST(p.payment_details.basic) b
  WHERE b.created_at BETWEEN b.payment_cycle_start_date AND b.payment_cycle_end_date
    AND b.status NOT IN ('UNCERTAIN')
  GROUP BY 1,2
), scoring AS (
  SELECT p.rider_id
    , p.country_code
    , MIN(s.payment_cycle_end_date) AS first_payroll_date
  FROM `{{ params.project_id }}.cl.rider_payments` p
  LEFT JOIN UNNEST(p.payment_details.scoring) s
  WHERE s.created_at BETWEEN s.payment_cycle_start_date AND s.payment_cycle_end_date
    AND s.status NOT IN ('UNCERTAIN')
  GROUP BY 1,2
), riders_payroll AS(
  SELECT q.rider_id
    , q.country_code
    , COALESCE(q.first_payroll_date, b.first_payroll_date, s.first_payroll_date) AS first_payroll_date
  FROM quest q
  LEFT JOIN basic b ON b.rider_id = q.rider_id
    AND b.country_code = q.country_code
  LEFT JOIN scoring s ON s.rider_id = q.rider_id
    AND s.country_code = q.country_code
)
SELECT CONCAT(r.country_code, '_', CAST(r.rider_id AS STRING)) AS external_id
    , ap.applicant_id
    , UPPER(r.country_code) AS rider_country
    , ci.city_name AS rider_city
    , ap.language_id as rider_language
    , r.email
    , r.phone_number
    , r.created_at AS hired_date
    , r.is_first_shift_scheduled AS first_shift_scheduled
    , r.first_shift_date
    , r.rider_name AS first_name
    , r.contract_type
    , od.vehicle_name AS vehicle_type
    , ap.referred_by_id
    , r.hours_worked AS total_hours
    , od.total_orders_delivered
    , r.total_shifts
    , rw.first_cod_date
    , rp.first_payroll_date
    , re.value AS referral_link
FROM riders r
LEFT JOIN applicants ap ON ap.country_code = r.country_code
  AND ap.rider_id = r.rider_id
LEFT JOIN orders od ON od.country_code = r.country_code
  AND od.rider_id = r.rider_id
LEFT JOIN cities ci ON ci.country_code = od.country_code
  AND ci.id = od.city_id
LEFT JOIN rider_wallet rw ON rw.country_code = r.country_code
  AND rw.rider_id = r.rider_id
LEFT JOIN rooster_employee_referral re ON re.country_code = r.country_code
  AND re.rider_id = r.rider_id
LEFT JOIN riders_payroll rp ON rp.country_code = r.country_code
  AND rp.rider_id = r.rider_id
