CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.rider_payments`
PARTITION BY created_date AS
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
  SELECT co.region
    , co.country_code
    , ci.id AS city_id
    , ci.name AS city_name
    , ci.timezone
  FROM `{{ params.project_id }}.cl.countries` co
  LEFT JOIN UNNEST (co.cities) ci
), no_utr AS (
  SELECT country_code
    , name AS starting_point_name
    , id AS starting_point_id
    , CASE
        WHEN country_code = 'se' THEN 'Europe/Stockholm'
        WHEN country_code = 'at' THEN 'Europe/Vienna'
        WHEN country_code = 'no' THEN 'Europe/Oslo'
        WHEN country_code = 'hk' THEN 'Asia/Hong_Kong'
        WHEN country_code = 'fi' THEN 'Europe/Helsinki'
        WHEN country_code = 'gr' THEN 'Europe/Athens'
      END AS timezone
  FROM `{{ params.project_id }}.dl.rooster_starting_point`
  WHERE name LIKE '%[NO*UTR]%'
    AND country_code IN ('se', 'at', 'no', 'hk', 'fi', 'gr')
  ORDER BY 1
), shifts AS (
  SELECT s.country_code
     , s.shift_id
     , s.city_id
     -- the following coalesce is needed to take shifts from NO*UTR starting points. These are special starting points
     -- for riders working partially in the office. These starting points do not have geo info thus they do not exist in `{{ params.project_id }}.cl.countries`
     , COALESCE(s.timezone, n.timezone) AS timezone
  FROM `{{ params.project_id }}.cl.shifts` s
  LEFT JOIN no_utr n USING(country_code, starting_point_id)
), riders AS (
  SELECT country_code
    , rider_id
    , rider_name
  FROM `{{ params.project_id }}.cl.riders`
), basic_payments AS (
  SELECT p.country_code
    , p.region
    , DATE(p.date_time, s.timezone) AS created_date
    , p.employee_id
    , r.rider_name
    , s.timezone
    , ce.exchange_rate
    , ROUND(CAST(SUM(SAFE_DIVIDE(d.total , ce.exchange_rate)) AS NUMERIC), 3) AS total_payment_eur
    , SUM(COALESCE(d.total, 0)) AS total_payment
    , ARRAY_AGG(STRUCT(d.payment_id
        , payment_rule_id
        , p.status
        , hu.hurrier_id AS delivery_id
        , p.shift_id
        , p.payment_cycle_id
        , p.payment_cycle_start_date
        , p.payment_cycle_end_date
        , payment_type
        , pr.name AS payment_rule_name
        , c.city_name
        , p.date_time AS created_at
        , d.total
        , ROUND(CAST(SAFE_DIVIDE(d.total, ce.exchange_rate) AS NUMERIC), 3) AS total_eur
        -- from 2020-06-05 the column is starting to be stored under paid_amount, countries in APAC will be gradually rolled out
        , COALESCE(d.paid_amount, d.amount) AS amount
    )) AS details
  FROM `{{ params.project_id }}.dl.rooster_payments_payment` p
  LEFT JOIN `{{ params.project_id }}.dl.rooster_payments_delivery` hu ON p.country_code = hu.country_code
    AND p.delivery_id = hu.id
  LEFT JOIN `{{ params.project_id }}.dl.rooster_payments_payment_detail` d  ON p.id = d.payment_id
    AND p.country_code = d.country_code
  LEFT JOIN shifts s ON p.shift_id = s.shift_id
    AND p.country_code = s.country_code
  LEFT JOIN cities c ON s.country_code = c.country_code
    AND c.city_id = s.city_id
  LEFT JOIN `{{ params.project_id }}.dl.rooster_payments_basic_payment_rule` pr ON pr.country_code = d.country_code
    AND pr.id = d.payment_rule_id
  LEFT JOIN riders r ON p.employee_id = r.rider_id
    AND p.country_code = r.country_code
  LEFT JOIN currencies ce ON p.country_code = ce.country_code
    AND DATE(p.date_time, s.timezone) = ce.created_date
  WHERE DATE(p.date_time, s.timezone) >= '2019-11-01'
  GROUP BY 1, 2, 3, 4, 5, 6, 7
), basic_payments_final AS (
  SELECT country_code
    , region
    , created_date
    , employee_id
    , rider_name
    , timezone
    , exchange_rate
    , total_payment_eur
    , total_payment
    , ARRAY(SELECT AS STRUCT payment_rule_id
        , SUM(total) AS total
        , SUM(total_eur) AS total_eur
        , SUM(amount) AS amount
      FROM UNNEST (details)
      GROUP BY  1
    ) AS details_by_rule
    , details
  FROM basic_payments
), hidden_basic_payments AS (
  SELECT p.country_code
    , p.region
    , DATE(p.date_time, s.timezone) AS created_date
    , p.employee_id
    , r.rider_name
    , s.timezone
    , ce.exchange_rate
    , ROUND(CAST(SUM(SAFE_DIVIDE(p.total , ce.exchange_rate)) AS NUMERIC), 3) AS total_payment_eur
    , SUM(COALESCE(p.total, 0)) AS total_payment
    , ARRAY_AGG(STRUCT(p.id AS payment_id
        , p.payment_rule_id
        , p.status
        , hu.hurrier_id AS delivery_id
        , p.shift_id
        , p.payment_cycle_id
        , p.payment_cycle_start_date
        , p.payment_cycle_end_date
        , payment_type
        , pr.name AS payment_rule_name
        , c.city_name
        , p.date_time AS created_at
        , p.total
        , ROUND(CAST(SAFE_DIVIDE(p.total, ce.exchange_rate) AS NUMERIC), 3) AS total_eur
        -- from 2020-06-05 the column is starting to be stored under paid_amount, countries in APAC will be gradually rolled out
        , COALESCE(p.paid_amount, p.amount) AS amount
    )) AS details
  FROM `{{ params.project_id }}.dl.rooster_payments_hidden_basic_payment` p
  LEFT JOIN `{{ params.project_id }}.dl.rooster_payments_delivery` hu ON p.country_code = hu.country_code
    AND p.delivery_id = hu.id
  LEFT JOIN shifts s ON p.shift_id = s.shift_id
    AND p.country_code = s.country_code
  LEFT JOIN cities c ON s.country_code = c.country_code
    AND c.city_id = s.city_id
  LEFT JOIN `{{ params.project_id }}.dl.rooster_payments_basic_payment_rule` pr ON pr.country_code = p.country_code
    AND pr.id = p.payment_rule_id
  LEFT JOIN riders r ON p.employee_id = r.rider_id
    AND p.country_code = r.country_code
  LEFT JOIN currencies ce ON p.country_code = ce.country_code
    AND DATE(p.date_time, s.timezone) = ce.created_date
  WHERE DATE(p.date_time, s.timezone) >= '2019-11-01'
  GROUP BY 1, 2, 3, 4, 5, 6, 7
), hidden_basic_payments_final AS (
  SELECT country_code
    , region
    , created_date
    , employee_id
    , rider_name
    , timezone
    , exchange_rate
    , total_payment_eur
    , total_payment
    , ARRAY(SELECT AS STRUCT payment_rule_id
        , SUM(total) AS total
        , SUM(total_eur) AS total_eur
        , SUM(amount) AS amount
      FROM UNNEST (details)
      GROUP BY  1
    ) AS details_by_rule
    , details
  FROM hidden_basic_payments
), quest_payments AS (
  SELECT qp.region
    , qp.country_code
    , qp.employee_id
    , r.rider_name
    , c.timezone
    , ce.exchange_rate
    , DATE(qp.date_time, c.timezone) AS created_date
    , ROUND(CAST(SUM(SAFE_DIVIDE(qp.total , ce.exchange_rate)) AS NUMERIC), 3) AS total_payment_eur
    , SUM(COALESCE(total, 0)) AS total_payment
    , ARRAY_AGG(STRUCT(qp.date_time AS created_at
        , qp.payment_rule_id
        , c.city_name
        , goal_id
        , threshold
        , qp.payment_cycle_id
        , qp.payment_cycle_start_date
        , qp.payment_cycle_end_date
        , qp.accepted_deliveries
        , qp.notified_deliveries
        , qp.no_shows
        , paid_period_start
        , paid_period_end
        , total
        , ROUND(CAST(SAFE_DIVIDE(qp.total, ce.exchange_rate) AS NUMERIC), 3) AS total_eur
        , qp.status
        , qpr.name AS quest_payment_rule_name
        , qpr.duration
        , qp.id AS payment_id
        , qpr.type AS payment_type
        , g.type AS goal_type
        , qp.amount
        , qp.acceptance_rate
    )) AS details
  FROM `{{ params.project_id }}.dl.rooster_payments_quest_payment` qp
  LEFT JOIN cities c ON qp.country_code = c.country_code
    AND qp.city_id = c.city_id
  LEFT JOIN `{{ params.project_id }}.dl.rooster_payments_goal` g ON qp.goal_id = g.id
    AND qp.country_code = g.country_code
    AND g.payment_type = 'QUEST'
  LEFT JOIN `{{ params.project_id }}.dl.rooster_payments_quest_payment_rule`  qpr  ON qpr.country_code  = qp.country_code
    AND qp.payment_rule_id = qpr.id
  LEFT JOIN riders r ON qp.country_code = r.country_code
    AND qp.employee_id = r.rider_id
  LEFT JOIN currencies ce ON qp.country_code = ce.country_code
    AND DATE(qp.date_time, c.timezone) = ce.created_date
  WHERE DATE(qp.date_time, c.timezone) >= '2019-11-01'
  GROUP BY  1, 2, 3, 4, 5, 6, 7
), quest_payments_final AS (
  SELECT region
    , country_code
    , employee_id
    , rider_name
    , created_date
    , timezone
    , exchange_rate
    , total_payment
    , total_payment_eur
    , ARRAY(SELECT AS STRUCT payment_rule_id
        , SUM(total) AS total
        , SUM(total_payment_eur) AS total_eur
        , SUM(amount) AS amount
      FROM UNNEST (details)
      GROUP BY  1
    ) AS details_by_rule
    , details
  FROM quest_payments
), scoring_payments_final AS (
  SELECT sp.region
    , sp.country_code
    , sp.employee_id
    , r.rider_name
    , DATE(sp.date_time, c.timezone) AS created_date
    , c.timezone
    , exchange_rate
    , ROUND(CAST(SUM(SAFE_DIVIDE(sp.total , ce.exchange_rate)) AS NUMERIC), 3) AS total_payment_eur
    , SUM(COALESCE(total, 0)) AS total_payment
    , ARRAY_AGG(STRUCT(sp.date_time AS created_at
        , sp.payment_rule_id
        , c.city_name
        , sp.scoring_amount
        , goal_id
        , sp.payment_cycle_id
        , sp.payment_cycle_start_date
        , sp.payment_cycle_end_date
        , paid_period_start
        , paid_period_end
        , total
        , ROUND(CAST(SAFE_DIVIDE(sp.total, ce.exchange_rate) AS NUMERIC), 3) AS total_eur
        , sp.status
        , sp.id AS payment_id
        , spr.type AS payment_type
        , g.type AS goal_type
        , sp.paid_unit_amount
        , hu.hurrier_id AS delivery_id
        , sp.evaluation_id
    )) AS details
  FROM `{{ params.project_id }}.dl.rooster_payments_real_time_scoring_payment` sp
  LEFT JOIN cities c ON sp.country_code = c.country_code
    AND sp.city_id = c.city_id
  LEFT JOIN `{{ params.project_id }}.dl.rooster_payments_goal` g ON sp.goal_id = g.id
    AND sp.country_code = g.country_code
    AND g.payment_type = 'SCORING'
  LEFT JOIN `{{ params.project_id }}.dl.rooster_payments_scoring_payment_rule` spr  ON spr.country_code  = sp.country_code
    AND sp.payment_rule_id = spr.id
  LEFT JOIN riders r ON sp.country_code = r.country_code
   AND sp.employee_id = r.rider_id
  LEFT JOIN `{{ params.project_id }}.dl.rooster_payments_delivery` hu ON sp.country_code = hu.country_code
    AND sp.delivery_id = hu.id
  LEFT JOIN currencies ce ON sp.country_code = ce.country_code
    AND DATE(sp.date_time, c.timezone) = ce.created_date
  WHERE DATE(sp.date_time, c.timezone) >= '2019-11-01'
  GROUP BY  1, 2, 3, 4, 5, 6, 7
), final_agg AS (
  SELECT b.country_code
    , b.region
    , b.created_date
    , b.employee_id
    , b.rider_name
    , b.timezone
    , b.exchange_rate
    , COALESCE(SUM(b.total_payment_eur), 0) + COALESCE(SUM(q.total_payment_eur), 0) + COALESCE(SUM(s.total_payment_eur), 0)
      + COALESCE(SUM(h.total_payment_eur), 0) AS total_date_payment_eur
    , COALESCE(SUM(b.total_payment), 0) + COALESCE(SUM(q.total_payment), 0)
      + COALESCE(SUM(s.total_payment), 0 ) + COALESCE(SUM(h.total_payment), 0) AS total_date_payment
  FROM basic_payments_final b
  LEFT JOIN quest_payments_final q ON b.country_code = q.country_code
    AND b.employee_id = q.employee_id
    AND b.created_date = q.created_date
  LEFT JOIN scoring_payments_final s ON b.country_code = s.country_code
    AND b.employee_id = s.employee_id
    AND b.created_date = s.created_date
  LEFT JOIN hidden_basic_payments_final h ON b.country_code = h.country_code
    AND b.employee_id = h.employee_id
    AND b.created_date = h.created_date
  GROUP BY 1, 2, 3, 4, 5, 6, 7
), dates AS (
  SELECT region
    , country_code
    , created_date
    , employee_id
    , rider_name
    , timezone
    , exchange_rate
  FROM basic_payments_final

  UNION DISTINCT

  SELECT region
    , country_code
    , created_date
    , employee_id
    , rider_name
    , timezone
    , exchange_rate
  FROM quest_payments_final

  UNION DISTINCT

  SELECT region
    , country_code
    , created_date
    , employee_id
    , rider_name
    , timezone
    , exchange_rate
  FROM scoring_payments_final

  UNION DISTINCT

  SELECT region
    , country_code
    , created_date
    , employee_id
    , rider_name
    , timezone
    , exchange_rate
  FROM hidden_basic_payments_final
)
SELECT d.region
  , d.country_code
  , d.created_date
  , d.employee_id AS rider_id
  , d.rider_name
  , d.timezone
  , d.exchange_rate
  , pi.total_date_payment_eur
  , pi.total_date_payment
  , STRUCT(b.details AS basic
     , b.details_by_rule AS basic_by_rule
     , q.details AS quest
     , q.details_by_rule AS quest_by_rule
     , s.details AS scoring
     , h.details AS hidden_basic
     , h.details_by_rule AS hidden_basic_by_rule
  ) AS payment_details
FROM dates d
LEFT JOIN basic_payments_final b ON b.country_code = d.country_code
  AND b.employee_id = d.employee_id
  AND b.created_date = d.created_date
LEFT JOIN quest_payments_final q ON q.country_code = d.country_code
  AND d.employee_id = q.employee_id
  AND d.created_date = q.created_date
LEFT JOIN scoring_payments_final s ON d.country_code = s.country_code
  AND d.employee_id = s.employee_id
  AND d.created_date = s.created_date
LEFT JOIN hidden_basic_payments_final h ON d.country_code = h.country_code
  AND d.employee_id = h.employee_id
  AND d.created_date = h.created_date
LEFT JOIN final_agg pi ON pi.country_code = d.country_code
  AND pi.employee_id = d.employee_id
  AND pi.created_date = d.created_date
