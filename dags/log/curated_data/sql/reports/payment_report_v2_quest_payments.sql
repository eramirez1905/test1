CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.payment_report_v2_quest_payments`
PARTITION BY report_date_local AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 8 WEEK) AS start_date
), batches AS (
  SELECT country_code
    , rider_id
    , b.number AS batch_number
    , b.active_from
    , b.active_until
  FROM `{{ params.project_id }}.cl.riders` r
  LEFT JOIN UNNEST(batches) b
), hurrier_deliveries AS (
  SELECT o.country_code
    , d.id AS delivery_id
    , d.rider_id
    , d.delivery_status
    , d.rider_near_customer_at
    , d.rider_accepted_at
    , d.city_id
    , ba.batch_number
    , DATETIME(d.rider_accepted_at, d.timezone) AS rider_accepted_at_local
    , DATE(d.rider_accepted_at, o.timezone) AS delivery_date
    , UPPER(FORMAT_DATE("%A", DATE(COALESCE(d.rider_near_customer_at, d.created_at)))) AS delivery_weekday
  FROM `{{ params.project_id }}.cl.orders` o
  LEFT JOIN UNNEST (deliveries) d
  LEFT JOIN batches ba ON o.country_code = ba.country_code
    AND d.rider_id = ba.rider_id
    AND d.rider_accepted_at BETWEEN ba.active_from AND ba.active_until
  WHERE o.created_date >= (SELECT start_date FROM parameters)
    AND d.delivery_status = 'completed'
), payment AS (
  SELECT p.region
    , p.country_code
    , p.rider_id
    , p.rider_name
    , d.city_id
    , d.batch_number
    , p.created_date AS payment_date
    , d.delivery_date
    , total_date_payment
    , d.delivery_id
    , d.delivery_weekday
    , d.delivery_status
    , d.rider_accepted_at
    , d.rider_accepted_at_local
    , DATETIME(s.actual_start_at, s.timezone) AS shift_actual_start_at
    , (SELECT SUM(total) FROM UNNEST(p.payment_details.quest_by_rule)) AS total
    , DATE(q.payment_cycle_start_date, p.timezone) AS payment_cycle_start_date
    , DATE(q.payment_cycle_end_date, p.timezone) AS payment_cycle_end_date
    , r.cost_factors AS cost_factors
    , r.applies_to AS applies_to
    , r.start_time
    , r.end_time
    , r.type
    , CASE r.type IN ('PER_DELIVERY', 'PER_DISTANCE', 'PER_KM')
        WHEN TIME(d.rider_accepted_at_local) BETWEEN r.start_time AND r.end_time
          AND (d.delivery_weekday IN UNNEST(a.days_of_week) OR ARRAY_LENGTH(a.days_of_week) = 0) THEN TRUE
        ELSE FALSE
      END as is_counted_per_delivery
   , CASE r.type IN ('PER_HOUR', 'PER_SHIFT')
        WHEN TIME(s.actual_start_at, s.timezone) BETWEEN r.start_time AND r.end_time
          AND (UPPER(FORMAT_DATE("%A", DATE(s.actual_start_at, s.timezone))) IN UNNEST(a.days_of_week) OR ARRAY_LENGTH(a.days_of_week) = 0) THEN TRUE
        ELSE FALSE
      END as is_counted_per_hour
  FROM `{{ params.project_id }}.cl.rider_payments` p
  LEFT JOIN UNNEST (payment_details.quest) q
  LEFT JOIN hurrier_deliveries d ON p.country_code = d.country_code
    AND d.rider_id = p.rider_id
    AND d.rider_accepted_at_local BETWEEN DATETIME(q.payment_cycle_start_date, p.timezone) AND DATETIME(q.payment_cycle_end_date, p.timezone)
  LEFT JOIN `{{ params.project_id }}.cl.payments_quest_rules` r ON p.country_code = r.country_code
    AND q.payment_rule_id = r.id
  LEFT JOIN UNNEST(applies_to) a
  LEFT JOIN `fulfillment-dwh-production.cl.shifts` s ON p.country_code = s.country_code
    AND p.rider_id = s.rider_id
    AND DATETIME(s.actual_start_at, s.timezone) BETWEEN DATE(q.payment_cycle_start_date, p.timezone) AND DATE(q.payment_cycle_end_date, p.timezone)
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
    , ARRAY_AGG(STRUCT(delivery_date
       , delivery_id
       , city_id
       , batch_number
       , is_counted_per_delivery
       , is_counted_per_hour
      )) deliveries_per_day
    , COUNT(DISTINCT(delivery_date)) AS days_counted
    , SAFE_DIVIDE(ANY_VALUE(total) , COUNT(DISTINCT(delivery_date))) AS cost_split_per_day
  FROM payment
  GROUP BY 1, 2, 3, 4, 5
), payments_split AS (
  SELECT type
    , country_code
    , rider_id
    , rider_name
    , p.payment_date
    , d.delivery_date
    , d.city_id
    , d.batch_number
    , COUNT(DISTINCT(delivery_id)) AS deliveries
   FROM costs p
   LEFT JOIN UNNEST(deliveries_per_day) d
   WHERE d.delivery_date <= p.payment_date
     AND is_counted_per_hour OR is_counted_per_delivery
   GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
   ORDER BY delivery_date
), payments_agg AS (
  SELECT c.type
    , c.country_code
    , c.rider_id
    , c.rider_name
    , c.payment_date
    , c.cost_split_per_day
    , c.days_counted
    , ARRAY_AGG(STRUCT(s.delivery_date
       , s.city_id
       , s.batch_number
       , s.deliveries
     )) AS deliveries_in_cycle
  FROM costs c
  LEFT JOIN payments_split s ON c.country_code = s.country_code
    AND s.rider_id = c.rider_id
    AND c.payment_date = s.payment_date
    AND s.type = c.type
  GROUP BY  1, 2, 3, 4, 5, 6, 7
)
SELECT country_code
  , rider_id
  , d.city_id
  , d.batch_number
  , d.delivery_date AS report_date_local
  , f.cost_split_per_day AS quest_payment_total
  , f.days_counted
  , d.deliveries
  , SAFE_DIVIDE(f.cost_split_per_day, d.deliveries) AS CPO_daily
FROM payments_agg f
LEFT JOIN UNNEST(deliveries_in_cycle) d
