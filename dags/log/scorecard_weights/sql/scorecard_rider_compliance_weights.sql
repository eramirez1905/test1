CREATE OR REPLACE TABLE rl.scorecard_rider_compliance_weights
PARTITION BY report_date_local AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 6 MONTH) AS start_time
    , DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY) AS end_time
), logs AS (
  SELECT a.country_code
    , o.city_id
    , CAST(DATETIME(a.created_at, a.timezone) AS DATE) AS report_date_local
    , COUNT(hurrier.order_code) AS undispatched_after_accepted ------ count all records, not all orders, do not use distinct
  FROM cl.audit_logs a
  LEFT JOIN cl.orders o ON a.country_code = o.country_code
    AND hurrier.order_code = o.platform_order_code
  WHERE a.created_date >= (SELECT start_time FROM parameters)
    AND action = 'manual_undispatch'
    AND hurrier.manual_undispatch.old_status = 'accepted'
  GROUP BY 1, 2, 3
), dataset AS (
  SELECT o.country_code
    , d.city_id
    , o.order_id
    , d.id AS delivery_id
    , delivery_status
    , COALESCE(DATE(DATETIME(d.rider_dropped_off_at, o.timezone)), DATE(DATETIME(d.created_at, o.timezone))) AS report_date_local
    , (SELECT COUNTIF(t.state = 'courier_notified') FROM UNNEST (deliveries) d LEFT JOIN d.transitions t) AS rider_notified_count
    , (SELECT COUNTIF(t.state = 'accepted') FROM UNNEST (deliveries) d LEFT JOIN d.transitions t) AS rider_accepted_count
    , (d.timings.at_customer_time / 60) AS at_customer_time
    , (d.timings.at_vendor_time / 60) AS at_vendor_time
    , (d.timings.rider_reaction_time / 60) AS rider_reaction_time
  FROM cl.orders o
  LEFT JOIN UNNEST (deliveries) d -- ON d.is_primary  --------- add condition? no, we want to have all deliveries
  WHERE o.created_date >= (SELECT start_time FROM parameters)
), deliveries AS (
  SELECT d.country_code
    , d.city_id
    , d.report_date_local
    , COUNT(DISTINCT delivery_id) AS count_deliveries
    , COUNT(DISTINCT(IF(delivery_status = 'completed', delivery_id, NULL))) AS completed_deliveries
    , COUNT(DISTINCT(IF(rider_reaction_time > 2, delivery_id, NULL))) AS reaction_time_over_2
    , COUNT(DISTINCT(IF(at_vendor_time > 7, delivery_id, NULL))) AS at_vendor_time_over_7
    , COUNT(DISTINCT(IF(at_customer_time > 5, delivery_id, NULL))) AS at_customer_time_over_5
    , SUM(rider_notified_count) AS notified_deliveries
    , SUM(rider_notified_count) - SUM(rider_accepted_count) AS unaccepted_deliveries
  FROM dataset d
  WHERE d.report_date_local BETWEEN (SELECT start_time FROM parameters) AND (SELECT end_time FROM parameters)
  GROUP BY 1, 2, 3
), shifts AS (
  SELECT s.country_code
    , s.city_id
    , DATE(DATETIME(s.shift_start_at, s.timezone)) AS report_date_local
    , SUM(IF(shift_state IN ('EVALUATED'), actual_working_time / 3600, 0)) AS hours_worked
    , SUM(IF(shift_state IN ('EVALUATED'), TIMESTAMP_DIFF(s.shift_end_at, s.shift_start_at, SECOND) / 3600, 0)) AS hours_planned
    , COUNT(DISTINCT(IF(shift_state IN ('NO_SHOW', 'EVALUATED'), shift_id, NULL))) AS all_shifts
    , COUNT(DISTINCT(IF(login_difference / 60 > 10 AND actual_working_time > 0, shift_id, NULL))) AS late_shifts
    , COUNT(DISTINCT(IF(shift_state = 'NO_SHOW', shift_id, NULL))) AS no_shows
  FROM cl.shifts s
  WHERE s.shift_state IN ('EVALUATED', 'NO_SHOW')
    AND s.created_date >= (SELECT start_time FROM parameters)
    AND DATE(DATETIME(s.shift_start_at, s.timezone)) BETWEEN (SELECT start_time FROM parameters) AND (SELECT end_time FROM parameters)
  GROUP BY 1, 2, 3
------------------------------ UTR --------------------------------
), utr AS (
  SELECT country_code
    , city_id
    , report_date_local
    , IF(delivery_status = 'completed', order_id, NULL) AS order_completed
    , NULL AS working_hours
  FROM dataset

  UNION ALL

  SELECT country_code
    , city_id
    , e.day AS report_date_local
    , NULL AS order_completed
    , (e.duration / 3600) AS working_hours
  FROM cl.shifts s
  LEFT JOIN UNNEST(s.actual_working_time_by_date) e
  WHERE s.created_date >= (SELECT start_time FROM parameters)
), utr_agg AS (
  SELECT country_code
    , city_id
    , report_date_local
    , COUNT(DISTINCT order_completed) AS orders_completed
    , SUM(working_hours) AS working_hours
  FROM utr
  GROUP BY 1, 2, 3
)
SELECT u.country_code
  , u.city_id
  , u.report_date_local
  , SAFE_DIVIDE(orders_completed, working_hours) AS utr
  , SAFE_DIVIDE(reaction_time_over_2, count_deliveries) AS perc_reactions_over_2
  , SAFE_DIVIDE(at_vendor_time_over_7, count_deliveries) AS perc_vendor_time_over_7
  , SAFE_DIVIDE(at_customer_time_over_5, count_deliveries) AS perc_customer_time_over_5
  , SAFE_DIVIDE(unaccepted_deliveries + l.undispatched_after_accepted, notified_deliveries) AS acceptance_issues
--   , SAFE_DIVIDE(late_shifts, all_shifts) AS perc_late_login_shifts
  , SAFE_DIVIDE(no_shows, all_shifts) AS perc_no_show_shifts
  , SAFE_DIVIDE(hours_worked, hours_planned) AS perc_full_work_shifts
FROM utr_agg u
LEFT JOIN deliveries d ON u.country_code = d.country_code
  AND u.city_id = d.city_id
  AND u.report_date_local = d.report_date_local
LEFT JOIN shifts s ON d.country_code = s.country_code
  AND d.city_id = s.city_id
  AND d.report_date_local = s.report_date_local
LEFT JOIN logs l ON u.country_code = l.country_code
  AND u.city_id = l.city_id
  AND u.report_date_local = l.report_date_local
;
