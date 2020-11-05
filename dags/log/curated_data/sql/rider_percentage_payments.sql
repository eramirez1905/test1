CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.rider_percentage_payments`
PARTITION BY created_date AS
WITH contract_cities AS (
  SELECT r.country_code
    , r.rider_id
    , r.rider_name
    , (SELECT city_id FROM UNNEST(contracts) WHERE status = 'VALID' AND CAST(start_at AS DATE) <= '{{ next_ds }}' ORDER BY end_at DESC LIMIT 1) AS city_id
  FROM `{{ params.project_id }}.cl.riders` r
)
SELECT p.region
  , p.country_code
  , DATE(p.date_time, ci.timezone) AS created_date
  , p.employee_id AS rider_id
  , c.rider_name
  , ci.timezone
  , SUM(COALESCE(p.total, 0)) AS total_payment_date
  , ARRAY_AGG(STRUCT(p.id AS payment_id
      , p.payment_rule_id
      , p.date_time AS created_at
      , p.total
      , p.payment_cycle_id
      , p.payment_cycle_start_date
      , p.payment_cycle_end_date
      , p.status
  )) AS payment_details
FROM `{{ params.project_id }}.dl.rooster_payments_percentage_payment` p
LEFT JOIN contract_cities c ON p.country_code = c.country_code
  AND p.employee_id = c.rider_id
LEFT JOIN `{{ params.project_id }}.cl.countries` co ON p.country_code = co.country_code
LEFT JOIN UNNEST (co.cities) ci ON c.city_id = ci.id
GROUP BY 1, 2, 3, 4, 5, 6
