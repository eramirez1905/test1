CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.payment_report_quest_scoring`
PARTITION BY report_date_local AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 1 YEAR) AS start_date
), hurrier_deliveries AS (
  SELECT o.country_code
    , d.id AS delivery_id
    , d.rider_id
    , d.delivery_status
    , d.rider_near_customer_at
    , d.rider_accepted_at
    , DATE(d.rider_accepted_at, o.timezone) AS delivery_date
    , UPPER(FORMAT_DATE("%A", DATE(COALESCE(d.rider_near_customer_at, d.created_at)))) AS delivery_weekday
  FROM `{{ params.project_id }}.cl.orders` o
  LEFT JOIN UNNEST (deliveries) d
  WHERE o.created_date >= (SELECT start_date FROM parameters)
    AND d.delivery_status = 'completed'
), payment AS (
  SELECT 'quest' AS type
    , p.region
    , p.country_code
    , p.rider_id
    , p.rider_name
    , p.created_date AS payment_date
    , d.delivery_date
    , total_date_payment
    , d.delivery_id
    , d.delivery_weekday
    , d.delivery_status
    , d.rider_accepted_at
    , NULL AS batch_number
    , (SELECT SUM(total) FROM UNNEST(p.payment_details.quest_by_rule)) AS total
    , q.payment_cycle_start_date
    , q.payment_cycle_end_date
  FROM `{{ params.project_id }}.cl.rider_payments` p
  LEFT JOIN UNNEST (payment_details.quest) q
  LEFT JOIN hurrier_deliveries d ON p.country_code = d.country_code
    AND d.rider_id = p.rider_id
    AND d.rider_accepted_at BETWEEN q.payment_cycle_start_date AND q.payment_cycle_end_date
  WHERE p.created_date >= (SELECT start_date FROM parameters)
    AND status = 'PAID'
  -- removing darkstore pickers to not impact utr as they do not have orders, only working hours
    AND p.country_code NOT LIKE '%dp%'

UNION ALL

  SELECT 'scoring' AS type
    , p.region
    , p.country_code
    , p.rider_id
    , p.rider_name
    , p.created_date AS payment_date
    , d.delivery_date
    , p.total_date_payment
    , d.delivery_id
    , d.delivery_weekday
    , d.delivery_status
    , d.rider_accepted_at
    , q.scoring_amount AS batch_number
    , (SELECT SUM(total) FROM UNNEST(p.payment_details.scoring)) AS total
    , q.payment_cycle_start_date
    , q.payment_cycle_end_date
  FROM `{{ params.project_id }}.cl.rider_payments` p
  LEFT JOIN UNNEST (payment_details.scoring) q
  LEFT JOIN hurrier_deliveries d ON p.country_code = d.country_code
    AND d.rider_id = p.rider_id
    AND d.rider_accepted_at BETWEEN q.payment_cycle_start_date AND q.payment_cycle_end_date
  WHERE p.created_date >= (SELECT start_date FROM parameters)
    AND q.status = 'PAID'
    -- removing darkstore pickers to not impact utr as they do not have orders, only working hours
    AND p.country_code NOT LIKE '%dp%'
), costs AS (
  SELECT type
    , country_code
    , rider_id
    , rider_name
    , payment_date
    , batch_number
    , ARRAY_AGG(STRUCT(delivery_date
       , delivery_id
      )) deliveries_per_day
    , COUNT(DISTINCT(delivery_date)) AS days_counted
    , SAFE_DIVIDE(ANY_VALUE(total) , COUNT(DISTINCT(delivery_date))) AS cost_split_per_day
  FROM payment
  GROUP BY 1, 2, 3, 4, 5, 6
), payments_split AS (
  SELECT type
    , country_code
    , rider_id
    , rider_name
    , batch_number
    , p.payment_date
    , d.delivery_date
    , COUNT(delivery_id) AS deliveries
   FROM costs p
   LEFT JOIN UNNEST(deliveries_per_day) d
   WHERE d.delivery_date <= p.payment_date
   GROUP BY 1, 2, 3, 4, 5, 6, 7
   ORDER BY delivery_date
), final AS (
  SELECT c.type
    , c.country_code
    , c.rider_id
    , c.rider_name
    , c.payment_date
    , c.cost_split_per_day
    , c.days_counted
    , c.batch_number
    , ARRAY_AGG(STRUCT(s.delivery_date
       , s.deliveries
     )) AS deliveries_in_cycle
  FROM costs c
  LEFT JOIN payments_split s ON c.country_code = s.country_code
    AND s.rider_id = c.rider_id
    AND c.payment_date = s.payment_date
    AND s.type = c.type
  GROUP BY  1, 2, 3, 4, 5, 6, 7, 8
)
SELECT type
  , country_code
  , rider_id
  , rider_name
  , d.delivery_date AS report_date_local
  , f.cost_split_per_day AS total
  , f.batch_number
  , f.days_counted
  , d.deliveries
  , SAFE_DIVIDE(f.cost_split_per_day, d.deliveries) AS CPO_daily
FROM final f
LEFT JOIN UNNEST(deliveries_in_cycle) d
