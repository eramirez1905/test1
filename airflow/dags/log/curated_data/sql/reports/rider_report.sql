CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.rider_report`
PARTITION BY report_date AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 6 WEEK) AS start_date
    , DATE('{{ next_ds }}') AS end_date
), contracts AS (   ---- contract information can be applied to every rider and should be joined in the last stage
  SELECT r1.country_code
    , r1.rider_id
    , r1.rider_name
    , r1.email
    , r1.batch_number AS current_batch_number
    , r1.created_at AS rider_created_at
    , r1.reporting_to AS captain_id
    , r2.rider_name AS captain_name
    , c.id
    , c.city_id
    , c.type
    , c.name
    , c.job_title
    , c.start_at
    , c.end_at
    , CASE
        WHEN c.end_at >= CURRENT_TIMESTAMP
          THEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP, c.start_at, DAY) / 7
        ELSE TIMESTAMP_DIFF(c.end_at, c.start_at, DAY) / 7
      END AS contract_duration_in_weeks
    , c.created_at
    , CASE
        WHEN c.name IS NULL
          THEN 'inactive' ELSE 'active'
        END AS rider_contract_status  ----- what does this mean ?
    , ROW_NUMBER() OVER(PARTITION BY r1.country_code, r1.rider_id ORDER BY start_at DESC, end_at DESC) AS rank_contract
  FROM `{{ params.project_id }}.cl.riders` r1
  LEFT JOIN `{{ params.project_id }}.cl.riders` r2 ON r1.country_code = r2.country_code
   AND r1.reporting_to = r2.rider_id
  LEFT JOIN UNNEST(r1.contracts) c
  WHERE c.status = 'VALID'
    AND DATE(c.start_at) <= '{{ next_ds }}' ---- consider only contracts which have already started
), tenure AS (   ---- tenure information can be applied to every rider and should be joined in the last stage
  SELECT country_code
    , rider_id
    , ROUND(SUM(contract_duration_in_weeks), 1) AS tenure_in_weeks
    , ROUND(SUM(contract_duration_in_weeks) / 52, 1) AS tenure_in_years
  FROM contracts
  GROUP BY 1, 2
), rider_shifts AS (
  SELECT country_code
    , rider_id
    , MIN(DATE(DATETIME(actual_start_at, timezone))) AS first_shift
    , MAX(DATE(DATETIME(actual_start_at, timezone))) AS last_shift
    , MIN(FORMAT_DATE('%G-%V', DATE(DATETIME(actual_start_at, timezone)))) AS first_shift_week
    , ARRAY_AGG(DISTINCT DATE(DATETIME(actual_start_at, timezone))) AS array_shift_day
    , ARRAY_AGG(DISTINCT FORMAT_DATE('%G-%V', DATE(DATETIME(actual_start_at, timezone)))) AS array_shift_week
  FROM `{{ params.project_id }}.cl.shifts`
  WHERE actual_start_at IS NOT NULL
    AND timezone IS NOT NULL
    AND created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 36 MONTH)
    AND shift_state = 'EVALUATED'
  GROUP BY 1, 2
), dimensions AS ( ---- add the dimensions which need to be associated to an order, a shift or which only apply in specific circumstances (entity, vehicle, batch number)
  SELECT DISTINCT report_date
    , country_code
    , entities
    , entity
    , city_id
    , zone_id
    , rider_id
    , vehicle_profile
    , vehicle_name
    , batch_number
  FROM `{{ params.project_id }}.cl._rider_report_source`
), final AS (
  SELECT report_date
    , country_code
    , entity
    , city_id
    , zone_id
    , rider_id
    , batch_number
    , vehicle_name
    , COUNT(DISTINCT(IF(shift_state = 'EVALUATED' AND hours_worked > 0, f.shift_id, NULL))) AS shifts_done
    , SUM(IF(f.shift_state = 'EVALUATED', hours_worked, 0)) AS hours_worked
    , SUM(hours_planned) AS hours_planned
    , SUM(break_time) AS break_time
    , COUNT(DISTINCT(IF(login_difference > 5 AND hours_worked > 0, f.shift_id, NULL))) AS late_shifts
    , COUNT(DISTINCT f.shift_id) AS all_shifts
    , COUNT(DISTINCT(IF(shift_state = 'NO_SHOW' OR shift_state = 'NO_SHOW_EXCUSED', f.shift_id, NULL))) AS no_shows
    -- 2020-08-28: bug found on rooster side. There are cases when 'NO_SHOW_EXCUSED' doesn't have an absence 
    -- thus it's unexcused. Therefore, add this further condition to unexcused_no_shows
    , COUNT(DISTINCT(IF(shift_state = 'NO_SHOW' OR shift_state = 'NO_SHOW_EXCUSED' AND is_unexcused, f.shift_id, NULL))) AS unexcused_no_shows
    , SUM(peak_time) AS peak_time
    , COUNT(DISTINCT(IF(is_weekend_shift AND hours_worked > 0 AND f.shift_state = 'EVALUATED', f.shift_id, NULL))) AS weekend_shifts
    , SUM(working_time) / 60 AS working_time
    , SUM(busy_time) / 60 AS busy_time
    , COUNT(DISTINCT(IF(delivery_status = 'completed', delivery_id, NULL))) AS count_deliveries
    , COUNT(DISTINCT(IF(delivery_status = 'cancelled', delivery_id, NULL))) AS cancelled_deliveries
    , COUNT(DISTINCT(IF(delivery_status = 'cancelled' AND cancellation_reason = 'TRAFFIC_MANAGER_UNABLE_FIND_CUSTOMER', delivery_id, NULL))) AS cancelled_deliveries_no_customer
    , COUNT(DISTINCT(IF(delivery_status = 'cancelled' AND rider_picked_up_at IS NOT NULL AND rider_near_customer_at IS NULL, delivery_id, NULL))) AS cancelled_deliveries_after_pickup
    , COUNT(DISTINCT(IF(rider_picked_up_at IS NOT NULL, delivery_id, NULL))) AS picked_up_deliveries
    , SUM(rider_notified_count) AS rider_notified
    , SUM(rider_accepted_count) AS rider_accepted
    , COUNT(DISTINCT(IF(log_status = 'accepted', delivery_id, NULL))) AS undispatched_after_accepted --- count of orders because order cannot be undispatched twice from the same rider, besides it is less expensive
    , SUM(IF(delivery_status = 'completed', actual_delivery_time / 60, NULL)) AS effective_delivery_time_sum
    , COUNT(IF(delivery_status = 'completed', actual_delivery_time / 60, NULL)) AS effective_delivery_time_count
    , SUM(at_vendor_time / 60) AS at_vendor_time_sum
    , COUNT(at_vendor_time / 60) AS at_vendor_time_count
    , SUM(at_customer_time / 60) AS at_customer_time_sum
    , COUNT(at_customer_time / 60) AS at_customer_time_count
    , SUM(IF(delivery_status = 'completed',dropoff_distance_manhattan / 1000, NULL)) AS dropoff_distance_sum
    , COUNT(IF(delivery_status = 'completed', dropoff_distance_manhattan / 1000, NULL)) AS dropoff_distance_count
    , SUM(IF(delivery_status = 'completed', pickup_distance_manhattan / 1000, NULL)) AS pickup_distance_sum
    , COUNT(IF(delivery_status = 'completed', pickup_distance_manhattan / 1000, NULL)) AS pickup_distance_count
    , SUM(rider_reaction_time) AS reaction_time_sec_sum
    , COUNT(rider_reaction_time) AS reaction_time_sec_count
    , COUNT(DISTINCT(IF(swap_status = 'ACCEPTED', f.shift_id, NULL))) AS swaps_accepted
    , COUNT(DISTINCT(IF(swap_status = 'PENDING' AND shift_state = 'NO_SHOW' OR shift_state = 'NO_SHOW_EXCUSED', f.shift_id, NULL))) AS swaps_pending_no_show
    , COUNT(DISTINCT(IF(swap_status = 'ACCEPTED' AND shift_state = 'NO_SHOW' OR shift_state = 'NO_SHOW_EXCUSED', f.shift_id, NULL))) AS swaps_accepted_no_show
  FROM `{{ params.project_id }}.cl._rider_report_source` f
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)
SELECT a.report_date
  , FORMAT_DATE('%G-%V', a.report_date) AS report_week
  , a.country_code
  , f.entities
  , co.country_name
  , a.entity
  , a.city_id
  , ci.name AS city_name
  , a.zone_id
  , z.name AS zone_name
  , a.rider_id
  , c.rider_name
  , c.email
  , a.batch_number
  , c.current_batch_number
  , c.type AS contract_type
  , c.name AS contract_name
  , c.job_title
  , DATE(DATETIME(c.start_at, ci.timezone)) AS contract_start_date_local
  , DATE(DATETIME(c.end_at, ci.timezone)) AS contract_end_date_local
  , FORMAT_DATE('%G-%V', DATE(DATETIME(c.end_at, ci.timezone))) AS contract_end_week
  , DATE(DATETIME(c.created_at, ci.timezone)) AS contract_creation_date_local
  , DATE(DATETIME(c.rider_created_at, ci.timezone)) AS hiring_date_local
  , c.rider_contract_status
  , c.captain_name
  , f.vehicle_profile
  , a.vehicle_name
  , te.tenure_in_weeks
  , te.tenure_in_years
  , IF(DATE_DIFF(rs.first_shift, DATE(DATETIME(c.start_at, ci.timezone)), DAY) >= 0, DATE_DIFF(rs.first_shift, DATE(DATETIME(c.start_at, ci.timezone)), DAY), NULL) AS time_to_street --- within period under consideration
  , a.shifts_done
  , a.hours_worked
  , a.hours_planned
  , a.break_time
  , a.late_shifts
  , a.all_shifts
  , a.no_shows
  , a.unexcused_no_shows
  , a.peak_time
  , a.weekend_shifts
  , a.working_time
  , a.busy_time
  , a.count_deliveries
  , a.cancelled_deliveries
  , a.cancelled_deliveries_no_customer
  , a.cancelled_deliveries_after_pickup
  , a.picked_up_deliveries
  , a.rider_notified
  , a.rider_accepted
  , a.undispatched_after_accepted --- count of orders because order cannot be undispatched twice from the same rider, besides it is less expensive
  , a.effective_delivery_time_sum
  , a.effective_delivery_time_count
  , a.at_vendor_time_sum
  , a.at_vendor_time_count
  , a.at_customer_time_sum
  , a.at_customer_time_count
  , a.dropoff_distance_sum
  , a.dropoff_distance_count
  , a.pickup_distance_sum
  , a.pickup_distance_count
  , a.reaction_time_sec_sum
  , a.reaction_time_sec_count
  , a.swaps_accepted
  , a.swaps_pending_no_show
  , a.swaps_accepted_no_show
FROM final a
LEFT JOIN dimensions f ON f.report_date = a.report_date
  AND f.country_code = a.country_code
  AND f.entity = a.entity
  AND f.city_id = a.city_id
  AND f.zone_id = a.zone_id
  AND f.rider_id = a.rider_id
  AND f.batch_number = a.batch_number
  AND f.vehicle_name = a.vehicle_name
LEFT JOIN contracts c ON a.country_code = c.country_code
  AND a.rider_id = c.rider_id
  AND c.rank_contract = 1
LEFT JOIN tenure te ON a.country_code = te.country_code
  AND a.rider_id = te.rider_id
LEFT JOIN rider_shifts rs ON a.country_code = rs.country_code
  AND a.rider_id = rs.rider_id
LEFT JOIN `{{ params.project_id }}.cl.countries` co ON a.country_code = co.country_code
LEFT JOIN UNNEST(co.cities) ci ON a.city_id = ci.id
LEFT JOIN UNNEST(ci.zones) z ON a.zone_id = z.id
-- removing darkstore pickers to not impact utr as they do not have orders, only working hours
WHERE a.country_code NOT LIKE '%dp%'
;
