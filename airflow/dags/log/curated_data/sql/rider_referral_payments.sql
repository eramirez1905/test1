CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.rider_referral_payments`
PARTITION BY created_date AS
WITH cities AS (
  SELECT country_code
    , ci.id AS city_id
    , ci.name AS city_name
    , ci.timezone AS timezone
  FROM `{{ params.project_id }}.cl.countries` co
  LEFT JOIN UNNEST(co.cities) ci
), riders AS (
  SELECT country_code
    , rider_id
    , rider_name
  FROM `{{ params.project_id }}.cl.riders`
)
SELECT p.region
  , p.country_code
  , DATE(p.date_time, timezone) AS created_date
  , p.employee_id AS rider_id
  , ri.rider_name
  , r.vehicle_type
  , r.contract_id
  , r.contract_type
  , c.city_name
  , r.city_id
  , c.timezone
  , SUM(COALESCE(p.total, 0)) AS total_payment_date
  , ARRAY_AGG(STRUCT(p.id AS payment_id
      , p.payment_rule_id
      , p.date_time AS created_at
      , r.referrer_id
      , r.referee_id
      , p.payment_cycle_id
      , p.payment_cycle_start_date
      , p.payment_cycle_end_date
      , p.paid_period_start
      , p.paid_period_end
      , p.total
      , p.unit_amount
      , p.status
  )) AS payment_details
FROM `{{ params.project_id }}.dl.rooster_payments_referral_payment` p
LEFT JOIN `{{ params.project_id }}.dl.rooster_payments_referral` r ON p.region = r.region
  AND p.country_code = r.country_code
  AND p.referral_id = r.id
LEFT JOIN cities c ON p.country_code = c.country_code
  AND r.city_id = c.city_id
LEFT JOIN riders ri ON p.country_code = ri.country_code
  AND p.employee_id = ri.rider_id
WHERE DATE(p.date_time, timezone) >= '2019-01-11'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
                     
