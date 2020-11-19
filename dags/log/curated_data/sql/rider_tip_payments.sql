CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.rider_tip_payments`
PARTITION BY created_date AS
WITH riders AS (
  SELECT r.country_code
    , r.rider_id
    , r.rider_name
  FROM `{{ params.project_id }}.cl.riders` r
), orders AS (
  SELECT o.country_code
    , o.order_id
    , o.timezone
  FROM `{{ params.project_id }}.cl.orders` o
)
SELECT p.region
  , p.country_code
  , o.timezone
  , DATE(p.date_time, o.timezone) AS created_date
  , p.employee_id AS rider_id
  , r.rider_name
  , SUM(COALESCE(total, 0)) AS total_payment_date
  , ARRAY_AGG(STRUCT(p.id AS payment_id
      , p.date_time AS created_at
      , p.order_id
      , d.hurrier_id AS delivery_id
      , p.order_tip
      , p.total 
      , p.payment_cycle_id
      , p.payment_cycle_start_date
      , p.payment_cycle_end_date
      , p.status
  )) AS payment_details
FROM `{{ params.project_id }}.dl.rooster_payments_tip_payment` p
LEFT JOIN riders r ON r.country_code = p.country_code
  AND r.rider_id = p.employee_id
LEFT JOIN orders o ON o.country_code = p.country_code
  AND o.order_id = p.order_id
LEFT JOIN `{{ params.project_id }}.dl.rooster_payments_delivery` d ON d.country_code = p.country_code
  AND d.id = p.delivery_id
GROUP BY 1, 2, 3, 4, 5, 6
