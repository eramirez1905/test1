CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.rider_kpi`
PARTITION BY created_date_local AS
WITH contracts AS (
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
    , IF(c.end_at >= '{{next_execution_date}}', TIMESTAMP_DIFF('{{next_execution_date}}', c.start_at, DAY) / 7,
      TIMESTAMP_DIFF(c.end_at, c.start_at, DAY) / 7) AS contract_duration_in_weeks
    , c.created_at
    , IF(c.name IS NULL, 'inactive', 'active') AS rider_contract_status
    , ROW_NUMBER() OVER(PARTITION BY r1.country_code, r1.rider_id ORDER BY start_at DESC, end_at DESC) AS rank_contract
  FROM `{{ params.project_id }}.cl.riders` r1
  LEFT JOIN `{{ params.project_id }}.cl.riders` r2 ON r1.country_code = r2.country_code
   AND r1.reporting_to = r2.rider_id
  LEFT JOIN UNNEST (r1.contracts) c
  WHERE c.status = 'VALID'
  -- consider only contracts which have already started
    AND DATE(c.start_at) <= '{{ next_ds }}'
-- tenure information can be applied to every rider and should be joined in the last stage
), countries_timezone AS (
  SELECT c.country_code
    -- taking one timezone per country to use in the final table
    , (SELECT timezone FROM UNNEST (cities) ci ORDER BY 1 LIMIT 1) AS timezone
  FROM `{{ params.project_id }}.cl.countries` c
), tenure AS (
  SELECT country_code
    , rider_id
    , ROUND(SUM(contract_duration_in_weeks), 1) AS tenure_in_weeks
    , ROUND(SUM(contract_duration_in_weeks) / 52, 1) AS tenure_in_years
  FROM contracts
  GROUP BY 1, 2
), rider_shifts AS (
  SELECT s.country_code
    , s.rider_id
    , MIN(DATE(s.actual_start_at, COALESCE(s.timezone, ct.timezone))) AS first_shift
    , MAX(DATE(s.actual_start_at, COALESCE(s.timezone, ct.timezone))) AS last_shift
    , MIN(FORMAT_DATE('%G-%V', DATE(DATETIME(s.actual_start_at, COALESCE(s.timezone, ct.timezone))))) AS first_shift_week
    , ARRAY_AGG(DISTINCT DATE(s.actual_start_at, COALESCE(s.timezone, ct.timezone))) AS array_shift_day
    , ARRAY_AGG(DISTINCT FORMAT_DATE('%G-%V', DATE(s.actual_start_at, COALESCE(s.timezone, ct.timezone)))) AS array_shift_week
  FROM `{{ params.project_id }}.cl.shifts` s
  LEFT JOIN countries_timezone ct ON s.country_code = ct.country_code
  WHERE actual_start_at IS NOT NULL
    AND shift_state = 'EVALUATED'
  GROUP BY 1, 2
-- add the dimensions which need to be associated to an order, a shift or which only apply in specific circumstances (entity, vehicle, batch number)
), dimensions AS (
  SELECT DISTINCT created_date_local
    , country_code
    , city_id
    , zone_id
    , rider_id
    , vehicle_profile
    , vehicle_name
    , batch_number
  FROM `{{ params.project_id }}.cl._rider_kpi_source`
  WHERE rider_id IS NOT NULL
), shifts AS (
  SELECT created_date_local
    , country_code
    , city_id
    , zone_id
    , rider_id
    , batch_number
    , vehicle_name
    , COUNT(DISTINCT(IF(shift_state = 'EVALUATED' AND working_time / 3600 > 0, f.shift_id, NULL))) AS shifts_done
    , SUM(IF(f.shift_state = 'EVALUATED', working_time, 0)) AS working_time
    , SUM(planned_working_time) AS planned_working_time
    , SUM(break_time) AS break_time
    , COUNT(DISTINCT(IF(login_difference > 5 AND working_time / 3600 > 0, f.shift_id, NULL))) AS late_shifts
    , COUNT(DISTINCT f.shift_id) AS all_shifts
    , COUNT(DISTINCT(IF(shift_state = 'NO_SHOW' OR shift_state = 'NO_SHOW_EXCUSED', f.shift_id, NULL))) AS no_shows
    -- 2020-08-28: bug found on rooster side. There are cases when 'NO_SHOW_EXCUSED' doesn't have an absence
    -- thus it's unexcused. Therefore, add this further condition to unexcused_no_shows
    , COUNT(DISTINCT(IF(shift_state = 'NO_SHOW' OR shift_state = 'NO_SHOW_EXCUSED' AND is_unexcused, f.shift_id, NULL))) AS unexcused_no_shows
    , SUM(peak_time) AS peak_time
    , COUNT(DISTINCT(IF(is_weekend_shift AND working_time / 3600 > 0 AND f.shift_state = 'EVALUATED', f.shift_id, NULL))) AS weekend_shifts
    , ROUND(SUM(transition_working_time), 2) AS transition_working_time
    , ROUND(SUM(transition_busy_time), 2) AS transition_busy_time
    , COUNT(DISTINCT(IF(swap_status = 'ACCEPTED', f.shift_id, NULL))) AS swaps_accepted
    , COUNT(DISTINCT(IF(swap_status = 'PENDING' AND shift_state = 'NO_SHOW'
      OR shift_state = 'NO_SHOW_EXCUSED', f.shift_id, NULL))) AS swaps_pending_no_show
    , COUNT(DISTINCT(IF(swap_status = 'ACCEPTED' AND shift_state = 'NO_SHOW'
      OR shift_state = 'NO_SHOW_EXCUSED', f.shift_id, NULL))) AS swaps_accepted_no_show
  FROM `{{ params.project_id }}.cl._rider_kpi_source` f
  GROUP BY 1, 2, 3, 4, 5, 6, 7
), source_entities AS (
  SELECT f.*
    , o.entity.display_name AS entity
    , o.entity.id AS entity_id
  FROM `{{ params.project_id }}.cl._rider_kpi_source` f
  LEFT JOIN `{{ params.project_id }}.cl.orders` o ON o.order_id = f.order_id
    AND f.country_code = o.country_code
  LEFT JOIN UNNEST (deliveries) d ON f.delivery_id = d.id
  WHERE COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone)) = f.created_date_local
), deliveries AS (
  SELECT e.created_date_local
    , e.country_code
    , e.city_id
    , e.zone_id
    , e.rider_id
    , e.batch_number
    , e.vehicle_name
    , e.entity
    , e.entity_id
    , COUNT(DISTINCT(IF(delivery_status = 'completed', delivery_id, NULL))) AS completed_deliveries
    , COUNT(DISTINCT(IF(delivery_status = 'cancelled', delivery_id, NULL))) AS cancelled_deliveries
    , COUNT(DISTINCT(IF(delivery_status = 'cancelled' AND cancellation_reason = 'TRAFFIC_MANAGER_UNABLE_FIND_CUSTOMER',
      delivery_id, NULL))) AS cancelled_deliveries_no_customer
    , COUNT(DISTINCT(IF(delivery_status = 'cancelled' AND rider_picked_up_at IS NOT NULL AND rider_near_customer_at IS NULL,
      delivery_id, NULL))) AS cancelled_deliveries_after_pickup
    , COUNT(DISTINCT(IF(rider_picked_up_at IS NOT NULL, delivery_id, NULL))) AS picked_up_deliveries
    , SUM(rider_notified_count) AS rider_notified
    , SUM(rider_accepted_count) AS rider_accepted
    -- count of orders because order cannot be undispatched twice from the same rider, besides it is less expensive
    , COUNT(DISTINCT(IF(log_status = 'accepted', delivery_id, NULL))) AS undispatched_after_accepted
    , SUM(IF(delivery_status = 'completed', actual_delivery_time, NULL)) AS effective_delivery_time_sum
    , COUNT(IF(delivery_status = 'completed', actual_delivery_time, NULL)) AS effective_delivery_time_count
    , SUM(at_vendor_time) AS at_vendor_time_sum
    , COUNT(at_vendor_time) AS at_vendor_time_count
    , SUM(at_customer_time) AS at_customer_time_sum
    , COUNT(at_customer_time) AS at_customer_time_count
    , ROUND(SUM(IF(delivery_status = 'completed',dropoff_distance_manhattan, NULL)), 2) AS dropoff_distance_sum
    , COUNT(IF(delivery_status = 'completed', dropoff_distance_manhattan, NULL)) AS dropoff_distance_count
    , ROUND(SUM(IF(delivery_status = 'completed', pickup_distance_manhattan, NULL)), 2) AS pickup_distance_sum
    , COUNT(IF(delivery_status = 'completed', pickup_distance_manhattan, NULL)) AS pickup_distance_count
    , SUM(rider_reaction_time) AS reaction_time_sec_sum
    , COUNT(rider_reaction_time) AS reaction_time_sec_count
    , COUNT(IF((at_customer_time / 60) < 5, e.delivery_id, NULL)) AS at_customer_time_under_5_count
    , COUNT(IF((at_customer_time / 60) >= 5 AND (at_customer_time / 60) < 10, e.delivery_id, NULL))
      AS at_customer_time_5_10_count
    , COUNT(IF((at_customer_time / 60) >= 10 AND (at_customer_time / 60) < 15, e.delivery_id, NULL))
      AS at_customer_time_10_15_count
    , COUNT(IF((at_customer_time / 60) >= 15, e.delivery_id, NULL)) AS at_customer_time_over_15_count
    , SUM(to_vendor_time) AS to_vendor_time_sum
    , COUNT(to_vendor_time) AS to_vendor_time_count
    , SUM(to_customer_time) AS to_customer_time_sum
    , COUNT(to_customer_time) AS to_customer_time_count
    , COUNT(IF(last_delivery_over_10 IS TRUE, delivery_id, NULL)) AS last_delivery_over_10_count
  FROM source_entities e
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
), aggregate_deliveries AS (
  SELECT created_date_local
    , country_code
    , city_id
    , zone_id
    , rider_id
    , batch_number
    , vehicle_name
    , ARRAY_AGG(
        STRUCT(entity
          , entity_id
          , completed_deliveries
          , cancelled_deliveries
          , cancelled_deliveries_no_customer
          , cancelled_deliveries_after_pickup
          , picked_up_deliveries
          , rider_notified
          , rider_accepted
          , undispatched_after_accepted
          , effective_delivery_time_sum
          , effective_delivery_time_count
          , at_vendor_time_sum
          , at_vendor_time_count
          , at_customer_time_sum
          , at_customer_time_count
          , dropoff_distance_sum
          , dropoff_distance_count
          , pickup_distance_sum
          , pickup_distance_count
          , reaction_time_sec_sum
          , reaction_time_sec_count
          , to_vendor_time_sum
          , to_vendor_time_count
          , to_customer_time_sum
          , to_customer_time_count
          , last_delivery_over_10_count
          , at_customer_time_under_5_count
          , at_customer_time_5_10_count
          , at_customer_time_10_15_count
          , at_customer_time_over_15_count
        )
      ) AS deliveries
  FROM deliveries
  GROUP BY 1, 2, 3, 4, 5, 6, 7
  )
SELECT d.created_date_local
  , d.country_code
  , co.country_name
  , d.city_id
  , ci.name AS city_name
  , d.zone_id
  , z.name AS zone_name
  , d.rider_id
  , c.rider_name
  , c.email
  , d.batch_number
  , c.current_batch_number
  , c.type AS contract_type
  , c.name AS contract_name
  , c.job_title
  , DATE(DATETIME(c.start_at, COALESCE(ci.timezone, ct.timezone))) AS contract_start_date_local
  , DATE(DATETIME(c.end_at, COALESCE(ci.timezone, ct.timezone))) AS contract_end_date_local
  , DATE(DATETIME(c.created_at, COALESCE(ci.timezone, ct.timezone))) AS contract_creation_date_local
  , DATE(DATETIME(c.rider_created_at, COALESCE(ci.timezone, ct.timezone))) AS hiring_date_local
  , c.rider_contract_status
  , c.captain_name
  , d.vehicle_profile
  , d.vehicle_name
  , te.tenure_in_weeks
  , te.tenure_in_years
  --- within period under consideration
  , IF(DATE_DIFF(rs.first_shift, DATE(DATETIME(c.start_at, COALESCE(ci.timezone, ct.timezone))), DAY) >= 0,
    DATE_DIFF(rs.first_shift, DATE(DATETIME(c.start_at, COALESCE(ci.timezone, ct.timezone))), DAY), NULL) AS time_to_street
  , s.shifts_done
  , s.working_time
  , s.planned_working_time
  , s.break_time
  , s.late_shifts
  , s.all_shifts
  , s.no_shows
  , s.unexcused_no_shows
  , s.peak_time
  , s.weekend_shifts
  , s.transition_working_time
  , s.transition_busy_time
  , s.swaps_accepted
  , s.swaps_pending_no_show
  , s.swaps_accepted_no_show
  , ad.deliveries
FROM dimensions d
LEFT JOIN shifts s ON s.created_date_local = d.created_date_local
  AND s.country_code = d.country_code
  AND (s.city_id = d.city_id OR (s.city_id IS NULL AND d.city_id IS NULL))
  AND (s.zone_id = d.zone_id OR (s.zone_id IS NULL AND d.zone_id IS NULL))
  AND s.rider_id = d.rider_id
  AND (s.batch_number = d.batch_number OR (s.batch_number IS NULL AND d.batch_number IS NULL))
  AND (s.vehicle_name = d.vehicle_name OR (s.vehicle_name IS NULL AND d.vehicle_name IS NULL))
LEFT JOIN aggregate_deliveries ad ON ad.created_date_local = d.created_date_local
  AND ad.country_code = d.country_code
  AND (ad.city_id = d.city_id OR (ad.city_id IS NULL AND d.city_id IS NULL))
  AND ad.rider_id = d.rider_id
  AND (ad.zone_id = d.zone_id OR (ad.zone_id IS NULL AND d.zone_id IS NULL))
  AND (ad.batch_number = d.batch_number OR (ad.batch_number IS NULL AND d.batch_number IS NULL))
  AND (ad.vehicle_name = d.vehicle_name OR (ad.vehicle_name IS NULL AND d.vehicle_name IS NULL))
LEFT JOIN contracts c ON c.country_code = d.country_code
  AND c.rider_id = d.rider_id
  AND c.rank_contract = 1
LEFT JOIN tenure te ON te.country_code = d.country_code
  AND te.rider_id = d.rider_id
LEFT JOIN rider_shifts rs ON rs.country_code = d.country_code
  AND rs.rider_id = d.rider_id
LEFT JOIN `{{ params.project_id }}.cl.countries` co ON co.country_code = d.country_code
LEFT JOIN UNNEST (co.cities) ci ON d.city_id = ci.id
LEFT JOIN UNNEST (ci.zones) z ON d.zone_id = z.id
LEFT JOIN countries_timezone ct ON d.country_code = ct.country_code
-- removing darkstore pickers to not impact utr as they do not have orders, only working hours
WHERE d.country_code NOT LIKE '%dp%'

