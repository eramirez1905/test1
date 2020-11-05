CREATE OR REPLACE TABLE  `{{ params.project_id }}.rl.payment_report_v2`
PARTITION BY report_date_local AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 8 WEEK) AS start_date
), currencies_dataset  AS (
  SELECT timestamp AS created_at
    , LEAD(timestamp) OVER (PARTITION BY currency_code ORDER BY timestamp ASC) AS previous_created_at
    , r.value
    , r.currency_code
    , r.value AS exchange_rate
  FROM  `{{ params.project_id }}.dl.data_fridge_currency_exchange`
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
), contracts AS (
  SELECT r.country_code
    , r.rider_id
    , (SELECT city_id FROM UNNEST(contracts) WHERE status = 'VALID' AND CAST(start_at AS DATE) <= CURRENT_DATE ORDER BY end_at DESC LIMIT 1) AS city_id
    , (SELECT name FROM UNNEST(contracts) WHERE status = 'VALID' AND CAST(start_at AS DATE) <= CURRENT_DATE ORDER BY end_at DESC LIMIT 1) AS contract_name
    , (SELECT type FROM UNNEST(contracts) WHERE status = 'VALID' AND CAST(start_at AS DATE) <= CURRENT_DATE ORDER BY end_at DESC LIMIT 1) AS contract_type
  FROM `{{ params.project_id }}.cl.riders` r
), batches AS (
  SELECT country_code
    , rider_id
    , b.number AS batch_number
    , b.active_from
    , b.active_until
  FROM `{{ params.project_id }}.cl.riders` r
  LEFT JOIN UNNEST(batches) b
), basic AS (
  SELECT s.zone_id
    , s.city_id
    , p.rider_id
    , p.created_date
    , p.country_code
    , p.exchange_rate
    , s.shift_id
    , deliveries.accepted AS accepted_deliveries
    , deliveries.notified AS notified_deliveries
    , ba.batch_number
    , b.created_at
    , TIMESTAMP_ADD(s.actual_end_at, INTERVAL 1 MINUTE) AS actual_shift_end_buffer
    , TIMESTAMP_ADD(b.created_at, INTERVAL 1 MINUTE) AS payment_created_at_buffer
    , (SELECT SUM(e.duration / 60 / 60) FROM UNNEST(s.actual_working_time_by_date) e WHERE e.status = 'ACCEPTED') AS working_hours
    , SUM(IF(b.payment_type = 'EVALUATION', b.total, 0)) AS per_hour
    , SUM(IF(b.payment_type = 'DELIVERY', b.total, 0)) AS per_delivery
    , SUM(IF(r.sub_type = 'NEAR_DROPOFF_DELIVERIES', b.total, 0)) AS per_near_dropoff_deliveries
    , SUM(IF(r.sub_type = 'PICKEDUP_DELIVERIES', b.total, 0)) AS per_picked_up_deliveries
    , SUM(IF(r.sub_type = 'NEAR_PICKUP_DELIVERIES', b.total, 0)) AS per_near_pickup_deliveries
    , SUM(IF(r.sub_type = 'ALL_DISTANCES', b.total, 0)) AS per_all_distances
    , SUM(IF(r.sub_type = 'DROPOFF_DISTANCES', b.total, 0)) AS per_dropoff_distances
    , SUM(IF(r.sub_type = 'COMPLETED_DELIVERIES', b.total, 0)) AS per_completed_deliveries
    , SUM(COALESCE(b.total, 0)) AS basic_total
  FROM `{{ params.project_id }}.cl.rider_payments` p
  LEFT JOIN UNNEST (payment_details.basic) b ON b.status IN ('PENDING', 'PAID')
  LEFT JOIN `{{ params.project_id }}.cl.shifts` s ON p.country_code = s.country_code
    -- join `{{ params.project_id }}.cl.shifts` on overlapping intervals by adding 1 minute to the payment and removing one minute to shift start and adding one minute to shift end.
    -- This is to cover deliveries accepted outside of evaluation.
    AND TIMESTAMP_ADD(b.created_at, INTERVAL 1 MINUTE) BETWEEN TIMESTAMP_SUB(s.actual_start_at, INTERVAL 1 MINUTE) AND TIMESTAMP_ADD(s.actual_end_at, INTERVAL 1 MINUTE)
    AND p.rider_id = s.rider_id
  LEFT JOIN batches ba ON p.country_code  = ba.country_code
    AND ba.rider_id = p.rider_id
    AND b.created_at BETWEEN ba.active_from AND ba.active_until
  LEFT JOIN `{{ params.project_id }}.cl.payments_basic_rules`  r ON p.country_code = r.country_code
    AND b.payment_rule_id = r.id
  WHERE p.created_date BETWEEN (SELECT start_date FROM parameters) AND DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY)
    AND p.country_code NOT LIKE '%dp%'
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
), basic_cleaned AS (
  SELECT country_code
    , exchange_rate
    , city_id
    , created_date
    , rider_id
    , shift_id
    , batch_number
    , working_hours
    , accepted_deliveries
    , notified_deliveries
    , per_hour
    , per_delivery
    , per_near_dropoff_deliveries
    , per_picked_up_deliveries
    , per_near_pickup_deliveries
    , per_all_distances
    , per_dropoff_distances
    , per_completed_deliveries
    , basic_total
    -- calculate interval with biggest difference between end of shift and payment timestamp to ensure we take only one entry in a deterministic way.
    , ROW_NUMBER() OVER (PARTITION BY country_code, rider_id, created_at, created_date ORDER BY TIMESTAMP_DIFF(actual_shift_end_buffer, payment_created_at_buffer, SECOND) DESC) AS overlap_rank
  FROM basic
), basic_agg AS (
  SELECT country_code
    , city_id
    , created_date
    , rider_id
    , shift_id
    , batch_number
    , accepted_deliveries
    , notified_deliveries
    , exchange_rate
    -- taking the max as hours are the same per multiple payments so we need any of those values. MAX serves as a way to aggregate
    , MAX(working_hours) AS working_hours
    , SUM(per_hour) AS per_hour
    , SUM(per_delivery) AS per_delivery
    , SUM(per_near_dropoff_deliveries) AS per_near_dropoff_deliveries
    , SUM(per_picked_up_deliveries) AS per_picked_up_deliveries
    , SUM(per_near_pickup_deliveries) AS per_near_pickup_deliveries
    , SUM(per_all_distances) AS per_all_distances
    , SUM(per_dropoff_distances) AS per_dropoff_distances
    , SUM(per_completed_deliveries) AS per_completed_deliveries
    , SUM(basic_total) AS basic_total
  FROM basic_cleaned
  WHERE overlap_rank = 1
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
), hidden_basic AS (
  SELECT s.zone_id
    , s.city_id
    , p.rider_id
    , p.created_date
    , p.country_code
    , p.exchange_rate
    , s.shift_id
    , deliveries.accepted AS accepted_deliveries
    , deliveries.notified AS notified_deliveries
    , ba.batch_number
    , hb.created_at
    , TIMESTAMP_ADD(s.actual_end_at, INTERVAL 1 MINUTE) AS actual_shift_end_buffer
    , TIMESTAMP_ADD(hb.created_at, INTERVAL 1 MINUTE) AS payment_created_at_buffer
    , (SELECT SUM(e.duration / 60 / 60) FROM UNNEST(s.actual_working_time_by_date) e WHERE e.status = 'ACCEPTED') AS working_hours
    , SUM(COALESCE(hb.total, 0)) AS hidden_basic_total
  FROM `{{ params.project_id }}.cl.rider_payments` p
  LEFT JOIN UNNEST (payment_details.hidden_basic) hb ON hb.status IN ('PENDING', 'PAID')
  LEFT JOIN `{{ params.project_id }}.cl.shifts` s ON p.country_code = s.country_code
    -- join `{{ params.project_id }}.cl.shifts` on overlapping intervals by adding 1 minute to the payment and removing one minute to shift start and adding one minute to shift end.
    -- This is to cover deliveries accepted outside of evaluation.
    AND TIMESTAMP_ADD(hb.created_at, INTERVAL 1 MINUTE) BETWEEN TIMESTAMP_SUB(s.actual_start_at, INTERVAL 1 MINUTE) AND TIMESTAMP_ADD(s.actual_end_at, INTERVAL 1 MINUTE)
    AND p.rider_id = s.rider_id
  LEFT JOIN batches ba ON p.country_code  = ba.country_code
    AND ba.rider_id = p.rider_id
    AND hb.created_at BETWEEN ba.active_from AND ba.active_until
  WHERE p.created_date BETWEEN (SELECT start_date FROM parameters) AND DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY)
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
), hidden_basic_cleaned AS (
  SELECT country_code
    , exchange_rate
    , city_id
    , created_date
    , rider_id
    , shift_id
    , batch_number
    , working_hours
    , accepted_deliveries
    , notified_deliveries
    , hidden_basic_total
    -- calculate interval with biggest difference between end of shift and payment timestamp to ensure we take only one entry in a deterministic way.
    , ROW_NUMBER() OVER (PARTITION BY country_code, rider_id, created_at, created_date ORDER BY TIMESTAMP_DIFF(actual_shift_end_buffer, payment_created_at_buffer, SECOND) DESC) AS overlap_rank
  FROM hidden_basic
), hidden_basic_agg AS (
  SELECT country_code
    , exchange_rate
    , city_id
    , created_date
    , rider_id
    , shift_id
    , batch_number
    , SUM(hidden_basic_total) AS hidden_basic_total
  FROM hidden_basic_cleaned
  WHERE overlap_rank = 1
  GROUP BY 1, 2, 3, 4, 5, 6, 7
), cpo_dataset AS (
  SELECT 'basic' AS _source
    , country_code
    , city_id
    , rider_id
    , created_date AS report_date_local
    , batch_number
    , exchange_rate
    , NULL AS working_hours
    , NULL AS accepted_deliveries
    , NULL AS notified_deliveries
    , CAST(SUM(basic_total) AS FLOAT64) AS basic_total
    , 0 AS hidden_basic_total
    , CAST(SUM(per_delivery) AS FLOAT64) AS basic_per_delivery
    , CAST(SUM(per_hour) AS FLOAT64) AS basic_per_hour
    , CAST(SUM(per_near_dropoff_deliveries) AS FLOAT64) AS per_near_dropoff_deliveries
    , CAST(SUM(per_picked_up_deliveries) AS FLOAT64) AS per_picked_up_deliveries
    , CAST(SUM(per_near_pickup_deliveries) AS FLOAT64) AS per_near_pickup_deliveries
    , CAST(SUM(per_all_distances) AS FLOAT64) AS per_all_distances
    , CAST(SUM(per_dropoff_distances) AS FLOAT64) AS per_dropoff_distances
    , CAST(SUM(per_completed_deliveries) AS FLOAT64) AS per_completed_deliveries
    , 0 AS tip_payment_total
    , 0 AS quest_payment_total
    , 0 AS scoring_payment_total
    , 0 AS percentage_payment_total
    , 0 AS referral_payment_total
    , 0 AS special_payment_total
    , NULL AS delivery_id
    , NULL AS pickup_distance_manhattan
    , NULL AS dropoff_distance_manhattan
 FROM basic_agg
 GROUP BY 1, 2, 3, 4, 5, 6, 7

 UNION ALL

 SELECT 'hidden_basic' AS _source
   , country_code
   , city_id
   , rider_id
   , created_date AS report_date_local
   , batch_number
   , exchange_rate
   , NULL AS working_hours
   , NULL AS accepted_deliveries
   , NULL AS notified_deliveries
   , 0 AS basic_total
   , CAST(SUM(hidden_basic_total) AS FLOAT64) AS hidden_basic_total
   , 0 AS basic_per_delivery
   , 0 AS basic_per_hour
   , 0 AS per_near_dropoff_deliveries
   , 0 AS per_picked_up_deliveries
   , 0 AS per_near_pickup_deliveries
   , 0 AS per_all_distances
   , 0 AS per_dropoff_distances
   , 0 AS per_completed_deliveries
   , 0 AS tip_payment_total
   , 0 AS quest_payment_total
   , 0 AS scoring_payment_total
   , 0 AS percentage_payment_total
   , 0 AS referral_payment_total
   , 0 AS special_payment_total
   , NULL AS delivery_id
   , NULL AS pickup_distance_manhattan
   , NULL AS dropoff_distance_manhattan
 FROM hidden_basic_agg
 GROUP BY 1, 2, 3, 4, 5, 6, 7

 UNION ALL

 SELECT 'percentage' AS _source
   , p.country_code
   , c.city_id
   , p.rider_id
   , created_date AS report_date_local
   , ba.batch_number
   , NULL AS exchange_rate
   , NULL AS working_hours
   , NULL AS accepted_deliveries
   , NULL AS notified_deliveries
   , 0 AS basic_total
   , 0 AS hidden_basic_total
   , 0 AS basic_per_delivery
   , 0 AS basic_per_hour
   , 0 AS per_near_dropoff_deliveries
   , 0 AS per_picked_up_deliveries
   , 0 AS per_near_pickup_deliveries
   , 0 AS per_all_distances
   , 0 AS per_dropoff_distances
   , 0 AS per_completed_deliveries
   , 0 AS tip_payment_total
   , 0 AS quest_payment_total
   , 0 AS scoring_payment_total
   , COALESCE(d.total, 0) AS percentage_payment_total
   , 0 AS referral_payment_total
   , 0 AS special_payment_total
   , NULL AS delivery_id
   , NULL AS pickup_distance_manhattan
   , NULL AS dropoff_distance_manhattan
 FROM `{{ params.project_id }}.cl.rider_percentage_payments` p
 LEFT JOIN UNNEST(payment_details) d
 LEFT JOIN batches ba ON p.country_code = ba.country_code
   AND ba.rider_id = p.rider_id
   AND d.created_at BETWEEN ba.active_from AND ba.active_until
 LEFT JOIN contracts c ON p.country_code = c.country_code
   AND p.rider_id = c.rider_id
 WHERE d.status IN ('PENDING', 'PAID')
   AND p.created_date BETWEEN (SELECT start_date FROM parameters) AND DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY)
   AND p.country_code NOT LIKE '%dp%'

 UNION ALL

 SELECT 'referral' AS _source
   , p.country_code
   , c.city_id
   , p.rider_id
   , created_date AS report_date_local
   , ba.batch_number
   , NULL AS exchange_rate
   , NULL AS working_hours
   , NULL AS accepted_deliveries
   , NULL AS notified_deliveries
   , 0 AS basic_total
   , 0 AS hidden_basic_total
   , 0 AS basic_per_delivery
   , 0 AS basic_per_hour
   , 0 AS per_near_dropoff_deliveries
   , 0 AS per_picked_up_deliveries
   , 0 AS per_near_pickup_deliveries
   , 0 AS per_all_distances
   , 0 AS per_dropoff_distances
   , 0 AS per_completed_deliveries
   , 0 AS tip_payment_total
   , 0 AS quest_payment_total
   , 0 AS scoring_payment_total
   , 0 AS percentage_payment_total
   , COALESCE(d.total, 0) AS referral_payment_total
   , 0 AS special_payment_total
   , NULL AS delivery_id
   , NULL AS pickup_distance_manhattan
   , NULL AS dropoff_distance_manhattan
 FROM `{{ params.project_id }}.cl.rider_referral_payments` p
 LEFT JOIN UNNEST(payment_details) d
 LEFT JOIN batches ba ON p.country_code = ba.country_code
   AND ba.rider_id = p.rider_id
   AND d.created_at BETWEEN ba.active_from AND ba.active_until
 LEFT JOIN contracts c ON p.country_code = c.country_code
   AND p.rider_id = c.rider_id
 WHERE d.status IN ('PENDING', 'PAID')
   AND p.created_date BETWEEN (SELECT start_date FROM parameters) AND DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY)
   AND p.country_code NOT LIKE '%dp%'

 UNION ALL

 SELECT 'tip' AS _source
   , p.country_code
   , c.city_id
   , p.rider_id
   , created_date AS report_date_local
   , ba.batch_number
   , NULL AS exchange_rate
   , NULL AS working_hours
   , NULL AS accepted_deliveries
   , NULL AS notified_deliveries
   , 0 AS basic_total
   , 0 AS hidden_basic_total
   , 0 AS basic_per_delivery
   , 0 AS basic_per_hour
   , 0 AS per_near_dropoff_deliveries
   , 0 AS per_picked_up_deliveries
   , 0 AS per_near_pickup_deliveries
   , 0 AS per_all_distances
   , 0 AS per_dropoff_distances
   , 0 AS per_completed_deliveries
   , COALESCE(d.total, 0) AS tip_payment_total
   , 0 AS quest_payment_total
   , 0 AS scoring_payment_total
   , 0 AS percentage_payment_total
   , 0 AS referral_payment_total
   , 0 AS special_payment_total
   , NULL AS delivery_id
   , NULL AS pickup_distance_manhattan
   , NULL AS dropoff_distance_manhattan
 FROM `{{ params.project_id }}.cl.rider_tip_payments` p
 LEFT JOIN UNNEST(payment_details) d
 LEFT JOIN batches ba ON p.country_code = ba.country_code
   AND ba.rider_id = p.rider_id
   AND d.created_at BETWEEN ba.active_from AND ba.active_until
 LEFT JOIN contracts c ON p.country_code = c.country_code
   AND p.rider_id = c.rider_id
 WHERE d.status IN ('PENDING', 'PAID')
   AND p.created_date BETWEEN (SELECT start_date FROM parameters) AND DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY)
   AND p.country_code NOT LIKE '%dp%'
 UNION ALL

 SELECT 'special' AS _source
   , country_code
   , city_id
   , rider_id
   , report_date_local
   , p.batch_number
   , NULL AS exchange_rate
   , NULL AS working_hours
   , NULL AS accepted_deliveries
   , NULL AS notified_deliveries
   , 0 AS basic_total
   , 0 AS hidden_basic_total
   , 0 AS basic_per_delivery
   , 0 AS basic_per_hour
   , 0 AS per_near_dropoff_deliveries
   , 0 AS per_picked_up_deliveries
   , 0 AS per_near_pickup_deliveries
   , 0 AS per_all_distances
   , 0 AS per_dropoff_distances
   , 0 AS per_completed_deliveries
   , 0 AS tip_payment_total
   , 0 AS quest_payment_total
   , 0 AS scoring_payment_total
   , 0 AS percentage_payment_total
   , 0 AS referral_payment_total
   , COALESCE(special_payment_total, 0) AS special_payment_total
   , NULL AS delivery_id
   , NULL AS pickup_distance_manhattan
   , NULL AS dropoff_distance_manhattan
 FROM `{{ params.project_id }}.rl.payment_report_v2_special_payments` p
 WHERE report_date_local BETWEEN (SELECT start_date FROM parameters) AND DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY)

 UNION ALL

 SELECT 'scoring' AS _source
   , p.country_code
   , city_id
   , p.rider_id
   , p.created_date AS report_date_local
   , sq.scoring_amount AS batch_number
   , NULL AS exchange_rate
   , NULL AS working_hours
   , NULL AS accepted_deliveries
   , NULL AS notified_deliveries
   , 0 AS basic_total
   , 0 AS hidden_basic_total
   , 0 AS basic_per_delivery
   , 0 AS basic_per_hour
   , 0 AS per_near_dropoff_deliveries
   , 0 AS per_picked_up_deliveries
   , 0 AS per_near_pickup_deliveries
   , 0 AS per_all_distances
   , 0 AS per_dropoff_distances
   , 0 AS per_completed_deliveries
   , 0 AS tip_payment_total
   , 0 AS quest_payment_total
   , COALESCE(sq.total, 0) AS scoring_payment_total
   , 0 AS percentage_payment_total
   , 0 AS referral_payment_total
   , 0 AS special_payment_total
   , NULL AS delivery_id
   , NULL AS pickup_distance_manhattan
   , NULL AS dropoff_distance_manhattan
 FROM `{{ params.project_id }}.cl.rider_payments` p
  LEFT JOIN UNNEST (payment_details.scoring) sq
 LEFT JOIN contracts c ON p.country_code = c.country_code
   AND p.rider_id = c.rider_id
 WHERE sq.status IN ('PENDING', 'PAID')
   AND p.created_date BETWEEN (SELECT start_date FROM parameters) AND DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY)
   AND p.country_code NOT LIKE '%dp%'

 UNION ALL

 SELECT 'quest' AS _source
   , country_code
   , city_id
   , rider_id
   , report_date_local
   , p.batch_number
   , NULL AS exchange_rate
   , NULL AS working_hours
   , NULL AS accepted_deliveries
   , NULL AS notified_deliveries
   , 0 AS basic_total
   , 0 AS hidden_basic_total
   , 0 AS basic_per_delivery
   , 0 AS basic_per_hour
   , 0 AS per_near_dropoff_deliveries
   , 0 AS per_picked_up_deliveries
   , 0 AS per_near_pickup_deliveries
   , 0 AS per_all_distances
   , 0 AS per_dropoff_distances
   , 0 AS per_completed_deliveries
   , 0 AS tip_payment_total
   , COALESCE(quest_payment_total, 0) AS quest_payment_total
   , 0 AS scoring_payment_total
   , 0 AS percentage_payment_total
   , 0 AS referral_payment_total
   , 0 AS special_payment_total
   , NULL AS delivery_id
   , NULL AS pickup_distance_manhattan
   , NULL AS dropoff_distance_manhattan
 FROM `{{ params.project_id }}.rl.payment_report_v2_quest_payments` p
 WHERE report_date_local BETWEEN (SELECT start_date FROM parameters) AND DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY)

 UNION ALL

 SELECT 'deliveries' AS _source
   , o.country_code
   , d.city_id
   , d.rider_id
   , DATE(d.rider_accepted_at, o.timezone) AS report_date_local
   , ba.batch_number
   , NULL AS exchange_rate
   , NULL AS working_hours
   , NULL AS accepted_deliveries
   , NULL AS notified_deliveries
   , 0 AS basic_total
   , 0 AS hidden_basic_total
   , 0 AS basic_per_delivery
   , 0 AS basic_per_hour
   , 0 AS per_near_dropoff_deliveries
   , 0 AS per_picked_up_deliveries
   , 0 AS per_near_pickup_deliveries
   , 0 AS per_all_distances
   , 0 AS per_dropoff_distances
   , 0 AS per_completed_deliveries
   , 0 AS tip_payment_total
   , 0 AS quest_payment_total
   , 0 AS scoring_payment_total
   , 0 AS percentage_payment_total
   , 0 AS referral_payment_total
   , 0 AS special_payment_total
   , d.id AS delivery_id
   , pickup_distance_manhattan / 1000 AS pickup_distance_manhattan
   , dropoff_distance_manhattan / 1000 AS dropoff_distance_manhattan
  FROM  `{{ params.project_id }}.cl.orders` o
  LEFT JOIN UNNEST (deliveries) d
  LEFT JOIN batches ba ON o.country_code = ba.country_code
    AND d.rider_id = ba.rider_id
    AND d.rider_accepted_at BETWEEN ba.active_from AND ba.active_until
  WHERE d.delivery_status = 'completed'
    AND o.created_date  >= (SELECT start_date FROM parameters)
    AND o.country_code NOT LIKE '%dp%'

 UNION ALL

 SELECT 'working_hours' AS _source
   , s.country_code
   , s.city_id
   , s.rider_id
   , DATE(s.actual_start_at, timezone) AS report_date_local
   , ba.batch_number
   , NULL AS exchange_rate
   , (e.duration / 60 / 60) working_hours
   , deliveries.accepted AS accepted_deliveries
   , deliveries.notified AS notified_deliveries
   , 0 AS basic_total
   , 0 AS hidden_basic_total
   , 0 AS basic_per_delivery
   , 0 AS basic_per_hour
   , 0 AS per_near_dropoff_deliveries
   , 0 AS per_picked_up_deliveries
   , 0 AS per_near_pickup_deliveries
   , 0 AS per_all_distances
   , 0 AS per_dropoff_distances
   , 0 AS per_completed_deliveries
   , 0 AS tip_payment_total
   , 0 AS quest_payment_total
   , 0 AS scoring_payment_total
   , 0 AS percentage_payment_total
   , 0 AS referral_payment_total
   , 0 AS special_payment_total
   , NULL AS delivery_id
   , NULL AS pickup_distance_manhattan
   , NULL AS dropoff_distance_manhattan
  FROM  `{{ params.project_id }}.cl.shifts` s
  LEFT JOIN UNNEST(s.actual_working_time_by_date) e
  LEFT JOIN batches ba ON s.country_code = ba.country_code
    AND s.rider_id = ba.rider_id
    AND s.actual_start_at BETWEEN ba.active_from AND ba.active_until
  WHERE s.shift_state = 'EVALUATED'
    AND e.status = 'ACCEPTED'
    AND DATE(s.actual_start_at, s.timezone) >= (SELECT start_date FROM parameters)
    AND s.country_code NOT LIKE '%dp%'
)
SELECT co.region
  , co.country_name
  , ci.name AS city_name
  , f.country_code
  , f.rider_id
  , con.contract_name
  , con.contract_type
  , f.report_date_local
  , FORMAT_DATE("%A", f.report_date_local) AS report_weekday
  , FORMAT_DATE("%G-%V", f.report_date_local) AS report_week
  , FORMAT_DATE("%Y-%m", f.report_date_local) AS report_month
  , f.batch_number
  , f.city_id
  , cur.exchange_rate
  , COUNT(delivery_id) AS deliveries
  , SUM(working_hours) AS working_hours
  , SUM(basic_total) AS basic_total
  , SUM(basic_per_delivery) AS basic_per_delivery
  , SUM(basic_per_hour) AS basic_per_hour
  , SUM(per_near_dropoff_deliveries) AS per_near_dropoff_deliveries
  , SUM(per_picked_up_deliveries) AS per_picked_up_deliveries
  , SUM(per_near_pickup_deliveries) AS per_near_pickup_deliveries
  , SUM(per_all_distances) AS per_all_distances
  , SUM(per_dropoff_distances) AS per_dropoff_distances
  , SUM(per_completed_deliveries) AS per_completed_deliveries
  , CAST(SUM(quest_payment_total) AS FLOAT64) AS quest_payment_total
  , CAST(SUM(scoring_payment_total) AS FLOAT64) AS scoring_payment_total
  , CAST(SUM(referral_payment_total) AS FLOAT64) AS referral_payment_total
  , CAST(SUM(special_payment_total) AS FLOAT64) AS special_payment_total
  , CAST(SUM(tip_payment_total) AS FLOAT64) AS tip_payment_total
  , CAST(SUM(percentage_payment_total) AS FLOAT64) AS percentage_payment_total
  , SUM(hidden_basic_total) AS hidden_basic_total
  , SAFE_DIVIDE(SUM(pickup_distance_manhattan), COUNT(pickup_distance_manhattan)) AS pickup_distance
  , SAFE_DIVIDE(SUM(dropoff_distance_manhattan), COUNT(dropoff_distance_manhattan)) AS dropoff_distance
  , ROUND(SAFE_DIVIDE(COUNT(delivery_id), SUM(working_hours)),1) AS UTR
  , SAFE_DIVIDE(SUM(accepted_deliveries), SUM(notified_deliveries)) AS acceptance_rate
  , SUM(accepted_deliveries) AS accepted_deliveries
  , SUM(notified_deliveries) AS notified_deliveries
  , SUM(pickup_distance_manhattan) AS pickup_distance_manhattan_sum
  , COUNT(pickup_distance_manhattan) AS pickup_distance_manhattan_count
  , SUM(dropoff_distance_manhattan) AS dropoff_distance_manhattan_sum
  , COUNT(dropoff_distance_manhattan) AS dropoff_distance_manhattan_count
FROM cpo_dataset f
LEFT JOIN `{{ params.project_id }}.cl.countries` co ON f.country_code = co.country_code
LEFT JOIN UNNEST (co.cities) ci ON f.city_id = ci.id
LEFT JOIN contracts con ON f.country_code = con.country_code
  AND con.rider_id = f.rider_id
LEFT JOIN currencies cur ON f.country_code = cur.country_code
  AND f.report_date_local = cur.created_date
WHERE f.report_date_local < '{{ next_ds }}'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
