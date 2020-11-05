CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._rider_weekly_costs` AS
WITH currencies_dataset AS (
  SELECT timestamp AS created_at
    , LEAD(timestamp) OVER (PARTITION BY currency_code ORDER BY timestamp ASC) AS previous_created_at
    , r.value
    , r.currency_code
    , r.value AS exchange_rate
  FROM `{{ params.project_id }}.dl.data_fridge_currency_exchange`
  LEFT JOIN UNNEST (rates) r
), countries AS (
  SELECT c.country_code
    , c.country_name
    , (SELECT timezone FROM UNNEST(cities) ORDER BY timezone LIMIT 1) AS timezone
    , c.currency_code
  FROM `{{ params.project_id }}.cl.countries` c
), currencies AS (
  SELECT c.country_code
    , COALESCE(DATE(e.created_at, timezone), DATE(e.previous_created_at, timezone)) AS created_date
    , e.value
    , e.currency_code
    , e.exchange_rate
  FROM currencies_dataset e
  LEFT JOIN countries c USING (currency_code)
), cities AS (
  SELECT r.country_code
    , r.rider_id
    , (SELECT city_id FROM UNNEST(contracts) WHERE status = 'VALID' AND CAST(start_at AS DATE) <= '{{ next_ds }}' ORDER BY end_at DESC LIMIT 1) AS city_id
  FROM `{{ params.project_id }}.cl.riders` r
), main_payments_dataset AS (
  SELECT p.region
    , p.country_code
    , p.created_date
    , p.rider_id
    , COALESCE((SELECT SUM(br.total) FROM UNNEST(payment_details.basic) br ),0) AS basic_total
    , COALESCE((SELECT SUM(total) FROM UNNEST(p.payment_details.quest) q WHERE q.status IN ('PENDING', 'PAID')), 0) AS quest_total
    , COALESCE((SELECT SUM(total) FROM UNNEST(p.payment_details.scoring) s WHERE s.status IN ('PENDING', 'PAID')), 0) AS scoring_total
    , COALESCE((SELECT SUM(total) FROM UNNEST(p.payment_details.hidden_basic) s WHERE s.status IN ('PENDING', 'PAID')), 0) AS hidden_basic_total
  FROM `{{ params.project_id }}.cl.rider_payments` p
  LEFT JOIN UNNEST (payment_details.basic) b ON b.status IN ('PENDING', 'PAID')
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
), special_payments AS (
  SELECT sp.country_code
    , sp.created_date
    , sp.rider_id
    , SUM(COALESCE(d.total, 0)) AS special_payment_total
  FROM `{{ params.project_id }}.cl.rider_special_payments` sp
  LEFT JOIN UNNEST(payment_details) d
  WHERE d.status IN ('PENDING', 'PAID')
  GROUP BY 1, 2, 3
), referral_payments AS (
  SELECT rp.country_code
    , rp.created_date
    , rp.rider_id
    , SUM(COALESCE(d.total, 0)) AS referral_payment_total
  FROM `{{ params.project_id }}.cl.rider_referral_payments` rp
  LEFT JOIN UNNEST(payment_details) d
  WHERE d.status IN ('PENDING', 'PAID')
  GROUP BY 1, 2, 3
), percentage_payments AS (
  SELECT pp.country_code
    , pp.created_date
    , pp.rider_id
    , SUM(COALESCE(d.total, 0)) AS percentage_payment_total
  FROM `{{ params.project_id }}.cl.rider_percentage_payments` pp
  LEFT JOIN UNNEST(payment_details) d
  WHERE d.status IN ('PENDING', 'PAID')
  GROUP BY 1, 2, 3
), tip_payments AS (
  SELECT tp.country_code
    , tp.created_date
    , tp.rider_id
    , SUM(COALESCE(d.total, 0)) AS tip_payment_total
  FROM `{{ params.project_id }}.cl.rider_tip_payments` tp
  LEFT JOIN UNNEST(payment_details) d
  WHERE d.status IN ('PENDING', 'PAID')
  GROUP BY 1, 2, 3
), dates AS (
  SELECT country_code
    , created_date
    , rider_id
  FROM main_payments_dataset

  UNION DISTINCT

  SELECT country_code
    , created_date
    , rider_id
  FROM special_payments

  UNION DISTINCT

  SELECT country_code
    , created_date
    , rider_id
  FROM referral_payments

  UNION DISTINCT

  SELECT country_code
    , created_date
    , rider_id
  FROM percentage_payments

  UNION DISTINCT

  SELECT country_code
    , created_date
    , rider_id
  FROM tip_payments
), aggregated_dataset AS (
  SELECT d.country_code
    , d.created_date
    , ci.city_id
    , c.exchange_rate
    , SUM(basic_total + quest_total + scoring_total + hidden_basic_total + COALESCE(special_payment_total, 0) + COALESCE(referral_payment_total, 0) + COALESCE(percentage_payment_total, 0) + COALESCE(tip_payment_total, 0)) AS total_costs
    , SUM(SAFE_DIVIDE(basic_total + quest_total + scoring_total + hidden_basic_total + COALESCE(special_payment_total, 0) + COALESCE(referral_payment_total, 0) + COALESCE(percentage_payment_total, 0) + COALESCE(tip_payment_total, 0), c.exchange_rate)) AS total_costs_eur
  FROM dates d
  LEFT JOIN main_payments_dataset b USING (country_code, created_date, rider_id)
  LEFT JOIN special_payments s USING (country_code, created_date, rider_id)
  LEFT JOIN referral_payments r USING (country_code, created_date, rider_id)
  LEFT JOIN percentage_payments p USING (country_code, created_date, rider_id)
  LEFT JOIN tip_payments t USING (country_code, created_date, rider_id)
  LEFT JOIN currencies c ON d.country_code = c.country_code
    AND c.created_date = d.created_date
  LEFT JOIN countries co ON d.country_code = co.country_code
  LEFT JOIN cities ci ON ci.country_code = d.country_code
    AND ci.rider_id = d.rider_id
  GROUP BY 1, 2, 3, 4
)
SELECT country_code
  , FORMAT_DATE("%G-%V", created_date) AS created_week
  , city_id
  , SUM(total_costs) AS total_costs
  , SUM(total_costs_eur) AS total_costs_eur
FROM aggregated_dataset
GROUP BY 1, 2, 3
