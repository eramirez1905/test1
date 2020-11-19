CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.rider_cohort`
PARTITION BY created_week_local AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 26 WEEK) AS start_date
), countries AS (
  SELECT country_code
    , region
    , country_name
    , ci.id AS city_id
    , ci.name AS city_name
    , ci.timezone AS timezone
  FROM `{{ params.project_id }}.cl.countries` co
  LEFT JOIN UNNEST(co.cities) ci
), utm_source AS (
  SELECT country_code
    , rider_id
    , c.name
    , c.value AS utm_source
  FROM `{{ params.project_id }}.cl.applicants` a
  LEFT JOIN UNNEST(custom_fields) c
  WHERE c.name = 'utm_source'
    AND rider_id IS NOT NULL
), cohort_dates AS (
  SELECT s.country_code
    , s.city_id
    , s.rider_id
    , IF(u.utm_source IS NULL, 'no_source', u.utm_source) AS utm_source
    , MIN(DATE(s.shift_start_at, s.timezone)) AS first_evaluated_local
    , DATE_TRUNC(MIN(DATE(s.shift_start_at, s.timezone)), WEEK) AS first_evaluated_week_local
  FROM `{{ params.project_id }}.cl.shifts` s
  LEFT JOIN utm_source u ON s.country_code = u.country_code
    AND s.rider_id = u.rider_id
  WHERE s.shift_state = 'EVALUATED'
  GROUP BY 1, 2, 3, 4
), cohort_size AS (
  SELECT cd.country_code
    , cd.city_id
    , cd.utm_source
    , cd.first_evaluated_week_local
    , COUNT(DISTINCT cd.rider_id) AS cohort_size
  FROM cohort_dates cd
  WHERE cd.first_evaluated_week_local >= (SELECT start_date FROM parameters)
  GROUP BY 1, 2, 3, 4
), rider_kpi_source AS (
  SELECT r.country_code
    , r.city_id
    , r.rider_id
    , r.batch_number
    , IF(u.utm_source IS NULL, 'no_source', u.utm_source) AS utm_source
    , DATE_TRUNC(r.created_date_local, WEEK) AS created_week_local
    , COUNT(DISTINCT(IF(shift_state = 'EVALUATED', r.rider_id, NULL))) AS riders
    , COUNT(DISTINCT(IF(shift_state = 'EVALUATED' AND working_time / 3600 > 0, r.shift_id, NULL))) AS shifts_done
    , SUM(IF(r.shift_state = 'EVALUATED', working_time, 0)) AS working_time
    , SUM(r.planned_working_time) AS planned_working_time
    , SUM(r.break_time) AS break_time
    , COUNT(DISTINCT(IF(login_difference > 5 AND working_time / 3600 > 0, r.shift_id, NULL))) AS late_shifts
    , COUNT(DISTINCT r.shift_id) AS all_shifts
    , COUNT(DISTINCT(IF(r.shift_state = 'NO_SHOW' OR r.shift_state = 'NO_SHOW_EXCUSED', r.shift_id, NULL))) AS no_shows
    -- 2020-08-28: bug found on rooster side. There are cases when 'NO_SHOW_EXCUSED' doesn't have an absence
    -- thus it's unexcused. Therefore, add this further condition to unexcused_no_shows
    , COUNT(DISTINCT(IF(r.shift_state = 'NO_SHOW' OR r.shift_state = 'NO_SHOW_EXCUSED' AND r.is_unexcused, r.shift_id, NULL))) AS unexcused_no_shows
    , SUM(r.peak_time) AS peak_time
    , COUNT(DISTINCT(IF(r.is_weekend_shift AND r.working_time / 3600 > 0 AND r.shift_state = 'EVALUATED', r.shift_id, NULL))) AS weekend_shifts
    , ROUND(SUM(r.transition_working_time), 2) AS transition_working_time
    , ROUND(SUM(r.transition_busy_time), 2) AS transition_busy_time
    , COUNT(DISTINCT(IF(delivery_status = 'completed', delivery_id, NULL))) AS completed_deliveries
    , COUNT(DISTINCT(IF(delivery_status = 'cancelled', delivery_id, NULL))) AS cancelled_deliveries
    , COUNT(DISTINCT(IF(delivery_status = 'cancelled' AND cancellation_reason = 'TRAFFIC_MANAGER_UNABLE_FIND_CUSTOMER', delivery_id, NULL))) AS cancelled_deliveries_no_customer
    , COUNT(DISTINCT(IF(delivery_status = 'cancelled' AND rider_picked_up_at IS NOT NULL AND rider_near_customer_at IS NULL, delivery_id, NULL))) AS cancelled_deliveries_after_pickup
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
  FROM `{{ params.project_id }}.cl._rider_kpi_source` r
  LEFT JOIN utm_source u ON r.country_code = u.country_code
    AND r.rider_id = u.rider_id
  WHERE r.created_date_local >= (SELECT start_date FROM parameters)
  GROUP BY 1, 2, 3, 4, 5, 6
)
SELECT r.country_code
  , c.country_name
  , c.region
  , r.city_id
  , c.city_name
  , r.utm_source
  , cd.first_evaluated_week_local
  , r.created_week_local
  , cz.cohort_size
  , DATE_DIFF(r.created_week_local, cd.first_evaluated_week_local, WEEK) AS date_diff
  , COUNT(DISTINCT(IF(riders > 0, r.rider_id, NULL))) AS number_of_riders
  , COUNT(DISTINCT(IF(batch_number = 1, r.rider_id, NULL))) AS batch_1
  , COUNT(DISTINCT(IF(batch_number = 2, r.rider_id, NULL))) AS batch_2
  , COUNT(DISTINCT(IF(batch_number = 3, r.rider_id, NULL))) AS batch_3
  , COUNT(DISTINCT(IF(batch_number = 4, r.rider_id, NULL))) AS batch_4
  , COUNT(DISTINCT(IF(batch_number = 5, r.rider_id, NULL))) AS batch_5
  , COUNT(DISTINCT(IF(batch_number = 6, r.rider_id, NULL))) AS batch_6
  , COUNT(DISTINCT(IF(batch_number = 7, r.rider_id, NULL))) AS batch_7
  , COUNT(DISTINCT(IF(batch_number IS NULL, r.rider_id, NULL))) AS batch_null
  , SUM(r.shifts_done) AS shifts_done
  , SUM(r.working_time) AS working_time
  , SUM(r.planned_working_time) AS planned_working_time
  , SUM(r.break_time) AS break_time
  , SUM(r.late_shifts) AS late_shifts
  , SUM(r.all_shifts) AS all_shifts
  , SUM(r.no_shows) AS no_shows
  , SUM(r.unexcused_no_shows) AS unexcused_no_shows
  , SUM(r.peak_time) AS peak_time
  , SUM(r.weekend_shifts) AS weekend_shifts
  , SUM(r.transition_working_time) AS transition_working_time
  , SUM(r.transition_busy_time) AS transition_busy_time
  , SUM(r.completed_deliveries) AS completed_deliveries
  , SUM(r.cancelled_deliveries) AS cancelled_deliveries
  , SUM(r.cancelled_deliveries_no_customer) AS cancelled_deliveries_no_customer
  , SUM(r.cancelled_deliveries_after_pickup) AS cancelled_deliveries_after_pickup
  , SUM(r.picked_up_deliveries) AS picked_up_deliveries
  , SUM(r.rider_notified) AS rider_notified
  , SUM(r.rider_accepted) AS rider_accepted
  , SUM(r.undispatched_after_accepted) AS undispatched_after_accepted
  , SUM(r.effective_delivery_time_sum) AS effective_delivery_time_n
  , SUM(r.effective_delivery_time_count) AS effective_delivery_time_d
  , SUM(r.at_vendor_time_sum) AS at_vendor_time_n
  , SUM(r.at_vendor_time_count) AS at_vendor_time_d
  , SUM(r.at_customer_time_sum) AS at_customer_time_n
  , SUM(r.at_customer_time_count) AS at_customer_time_d
FROM rider_kpi_source r
LEFT JOIN countries c ON r.country_code = c.country_code
  AND r.city_id = c.city_id
INNER JOIN cohort_dates cd ON r.country_code = cd.country_code
  AND r.city_id = cd.city_id
  AND r.rider_id = cd.rider_id
  AND r.utm_source = cd.utm_source
LEFT JOIN cohort_size cz ON cd.country_code = cz.country_code
  AND cd.city_id = cz.city_id
  AND cd.utm_source = cz.utm_source
  AND cd.first_evaluated_week_local = cz.first_evaluated_week_local
WHERE cd.first_evaluated_week_local >= (SELECT start_date FROM parameters)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
