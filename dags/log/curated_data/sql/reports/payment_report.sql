CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.payment_report`
PARTITION BY report_date_local AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 1 YEAR) AS start_date
), countries AS (
  SELECT country_code
    , ci.id AS city_id
    , ci.timezone AS timezone
    , zo.id AS zone_id
    , zo.name AS zone_name
    , sp.id AS starting_point_id
  FROM `{{ params.project_id }}.cl.countries` co
  LEFT JOIN UNNEST(co.cities) ci
  LEFT JOIN UNNEST(ci.zones) zo
  LEFT JOIN UNNEST (zo.starting_points) sp
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
    , (SELECT vehicle_name FROM UNNEST(s.actual_working_time_by_date) e WHERE e.day = DATE(s.actual_start_at, s.timezone) AND e.status = 'ACCEPTED' ORDER BY e.duration DESC LIMIT 1 ) AS vehicle_name
    , (SELECT SUM(e.duration / 60 / 60) FROM UNNEST(s.actual_working_time_by_date) e WHERE e.status = 'ACCEPTED') AS working_hours
    , COUNT(DISTINCT zone_id) OVER (PARTITION BY p.country_code, p.rider_id, p.created_date) AS zone_partition
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
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
), basic_cleaned AS (
  SELECT country_code
    , exchange_rate
    , zone_id
    , city_id
    , created_date
    , rider_id
    , shift_id
    , batch_number
    , vehicle_name
    , working_hours
    , zone_partition
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
), basic_final AS (
  SELECT country_code
    , exchange_rate
    , zone_id
    , city_id
    , created_date
    , rider_id
    , shift_id
    , batch_number
    , vehicle_name
    , zone_partition
    , accepted_deliveries
    , notified_deliveries
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
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
), quest AS (
  SELECT sq.rider_id
    , sq.report_date_local
    , sq.country_code
    , sq.batch_number
    , SUM(sq.total) AS quest_total
  FROM `{{ params.project_id }}.rl.payment_report_quest_scoring` sq
  WHERE type = 'quest'
  GROUP BY 1, 2, 3, 4
),  scoring AS (
  SELECT p.rider_id
    , p.created_date AS report_date_local
    , p.country_code
    , p.exchange_rate
    , sq.scoring_amount AS batch_number
    , SUM(sq.total) AS scoring_total
  FROM `{{ params.project_id }}.cl.rider_payments` p
  LEFT JOIN UNNEST (payment_details.scoring) sq
  WHERE status IN ('PENDING', 'PAID')
  GROUP BY 1, 2, 3, 4, 5
), final AS (
  SELECT 'payments' AS _source
    , p.country_code
    , p.exchange_rate
    , p.zone_id
    , p.city_id
    , p.created_date AS report_date_local
    , FORMAT_DATE("%A", p.created_date) AS report_weekday
    , FORMAT_DATE("%G-%V", p.created_date) AS report_week
    , FORMAT_DATE("%Y-%m", p.created_date) AS report_month
    , p.rider_id
    , COALESCE(sq.batch_number, p.batch_number) AS batch_number
    , p.vehicle_name
    , (SELECT name FROM UNNEST(contracts) WHERE status = 'VALID' AND CAST(start_at AS DATE) <= '{{ next_ds }}' ORDER BY end_at DESC LIMIT 1) AS contract_name
    , (SELECT type FROM UNNEST(contracts) WHERE status = 'VALID' AND CAST(start_at AS DATE) <= '{{ next_ds }}' ORDER BY end_at DESC LIMIT 1) AS contract_type
    , SUM(p.working_hours) AS working_hours
    , SUM(p.accepted_deliveries) AS accepted_deliveries
    , SUM(p.notified_deliveries) AS notified_deliveries
    , CAST(SUM(basic_total) AS FLOAT64) AS basic_total
    , CAST(SUM(per_delivery) AS FLOAT64) AS basic_per_delivery
    , CAST(SUM(per_hour) AS FLOAT64) AS basic_per_hour
    , CAST(SUM(per_near_dropoff_deliveries) AS FLOAT64) AS per_near_dropoff_deliveries
    , CAST(SUM(per_picked_up_deliveries) AS FLOAT64) AS per_picked_up_deliveries
    , CAST(SUM(per_near_pickup_deliveries) AS FLOAT64) AS per_near_pickup_deliveries
    , CAST(SUM(per_all_distances) AS FLOAT64) AS per_all_distances
    , CAST(SUM(per_dropoff_distances) AS FLOAT64) AS per_dropoff_distances
    , CAST(SUM(per_completed_deliveries) AS FLOAT64) AS per_completed_deliveries
    , COALESCE(CAST(SAFE_DIVIDE(ANY_VALUE(quest_total), ANY_VALUE(zone_partition)) AS FLOAT64), 0) AS quest_total
    , COALESCE(CAST(SAFE_DIVIDE(ANY_VALUE(scoring_total), ANY_VALUE(zone_partition)) AS FLOAT64), 0) AS scoring_total
    , NULL AS deliveries
    , NULL AS pickup_distance_manhattan_sum
    , NULL AS pickup_distance_manhattan_count
    , NULL AS dropoff_distance_manhattan_sum
    , NULL AS dropoff_distance_manhattan_count
  FROM basic_final p
  LEFT JOIN quest q ON p.country_code = q.country_code
    AND q.report_date_local = p.created_date
    AND q.rider_id = p.rider_id
  LEFT JOIN scoring sq ON p.country_code = sq.country_code
    AND sq.report_date_local = p.created_date
    AND sq.rider_id = p.rider_id
  LEFT JOIN `{{ params.project_id }}.cl.riders` r ON p.country_code = r.country_code
    AND r.rider_id = p.rider_id
  WHERE p.created_date >= (SELECT start_date FROM parameters)
  -- removing darkstore pickers to not impact utr as they do not have orders, only working hours
  AND p.country_code NOT LIKE '%dp%'
  GROUP BY 1, 2, 3, 4, 5, 6 , 7 , 8 , 9, 10, 11, 12, 13, 13, 14

UNION ALL

  SELECT 'deliveries' AS _source
    , o.country_code
    , CAST(NULL AS FLOAT64) AS exchange_rate
    , c.zone_id
    , d.city_id
    , DATE(d.rider_accepted_at, o.timezone) AS report_date_local
    , FORMAT_DATE("%A",DATE(d.rider_accepted_at, o.timezone)) AS report_weekday
    , FORMAT_DATE("%G-%V", DATE(d.rider_accepted_at, o.timezone)) AS report_week
    , FORMAT_DATE("%Y-%m", DATE(d.rider_accepted_at, o.timezone)) AS report_month
    , d.rider_id
    , ba.batch_number
    , d.vehicle.name AS vehicle_name
    , (SELECT name FROM UNNEST(contracts) WHERE status = 'VALID' AND CAST(start_at AS DATE) <= '{{ next_ds }}' ORDER BY end_at DESC LIMIT 1) AS contract_name
    , (SELECT type FROM UNNEST(contracts) WHERE status = 'VALID' AND CAST(start_at AS DATE) <= '{{ next_ds }}' ORDER BY end_at DESC LIMIT 1) AS contract_type
    , NULL AS working_hours
    , NULL AS accepted_deliveries
    , NULL AS notified_deliveries
    , NULL AS basic_total
    , NULL AS basic_per_delivery
    , NULL AS basic_per_hour
    , NULL AS per_near_dropoff_deliveries
    , NULL AS per_picked_up_deliveries
    , NULL AS per_near_pickup_deliveries
    , NULL AS per_all_distances
    , NULL AS per_dropoff_distances
    , NULL AS per_completed_deliveries
    , NULL AS quest_total
    , NULL AS scoring_total
    , COUNT(d.id) AS deliveries
    , SUM(pickup_distance_manhattan / 1000) AS pickup_distance_manhattan_sum
    , COUNT(pickup_distance_manhattan / 1000) AS pickup_distance_manhattan_count
    , SUM(dropoff_distance_manhattan / 1000) AS dropoff_distance_manhattan_sum
    , COUNT(dropoff_distance_manhattan / 1000) AS dropoff_distance_manhattan_count
  FROM `{{ params.project_id }}.cl.orders` o
  LEFT JOIN UNNEST (deliveries) d
  LEFT JOIN countries c ON d.rider_starting_point_id = c.starting_point_id
    AND o.country_code = c.country_code
  LEFT JOIN batches ba ON o.country_code = ba.country_code
    AND d.rider_id = ba.rider_id
    AND d.rider_accepted_at BETWEEN ba.active_from AND ba.active_until
  LEFT JOIN `{{ params.project_id }}.cl.riders` r ON o.country_code = r.country_code
    AND d.rider_id = r.rider_id
  WHERE d.delivery_status = 'completed'
    AND o.created_date  >= (SELECT start_date FROM parameters)
    -- removing darkstore pickers to not impact utr as they do not have orders, only working hours
    AND o.country_code NOT LIKE '%dp%'
  GROUP BY  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
)

SELECT co.region
  , co.country_name
  , ci.name AS city_name
  , z.name AS zone_name
  , f.country_code
  , f.rider_id
  , f.vehicle_name
  , f.contract_name
  , f.contract_type
  , f.report_date_local
  , f.report_weekday
  , f.report_week
  , f.report_month
  , f.batch_number
  , f.zone_id
  , f.city_id
  , f.exchange_rate
  , SUM(deliveries) AS deliveries
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
  , SUM(quest_total) AS quest_total
  , SUM(scoring_total) AS scoring_total
  , SUM(basic_total) + SUM(quest_total) + SUM(scoring_total) AS total_date_payment
  , SAFE_DIVIDE(SUM(pickup_distance_manhattan_sum), SUM(pickup_distance_manhattan_count)) AS pickup_distance
  , SAFE_DIVIDE(SUM(dropoff_distance_manhattan_sum), SUM(dropoff_distance_manhattan_count)) AS dropoff_distance
  , ROUND(SAFE_DIVIDE(SUM(deliveries), SUM(working_hours)),1) AS UTR
  , SAFE_DIVIDE(SUM(accepted_deliveries), SUM(notified_deliveries)) AS acceptance_rate
FROM final f
LEFT JOIN `{{ params.project_id }}.cl.countries` co ON f.country_code = co.country_code
LEFT JOIN UNNEST (co.cities) ci ON f.city_id = ci.id
LEFT JOIN UNNEST (ci.zones) z ON f.zone_id = z.id
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
