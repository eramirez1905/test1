CREATE OR REPLACE TABLE rl.scorecard_staffing_weights
PARTITION BY report_date_local AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 6 MONTH) AS start_time
    , DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY) AS end_time
), dates AS (
  SELECT date
  FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 200 DAY), DAY), TIMESTAMP_TRUNC(CURRENT_TIMESTAMP, HOUR), INTERVAL 15 MINUTE)) AS date
), countries AS (
  SELECT co.country_code
    , co.country_name
    , ci.id AS city_id
    , ci.name AS city_name
    , ci.timezone
    , z.id AS zone_id
    , z.name AS zone_name
  FROM cl.countries co
  LEFT JOIN UNNEST(cities) ci
  LEFT JOIN UNNEST(ci.zones) z
), report_dates AS (
  SELECT CAST(date AS DATE) AS report_date
    , date AS start_datetime
    , TIMESTAMP_ADD(date, INTERVAL 15 MINUTE) AS end_datetime
  FROM dates
), manual_slots AS (
  SELECT DISTINCT s.country_code
    , s.city_id
    , dates.report_date
    , dates.start_datetime AS start_datetime_local
    , COUNTIF(s.tag = 'MANUAL' AND s.parent_id IS NULL) AS manual_assigned
  FROM report_dates dates
  LEFT JOIN il.staffing s ON dates.report_date = CAST(DATETIME(s.start_time, s.timezone) AS DATE)
    AND CAST(DATETIME(s.start_time, s.timezone) AS TIMESTAMP) < dates.end_datetime
    AND CAST(DATETIME(s.end_time, s.timezone) AS TIMESTAMP) > dates.start_datetime
  GROUP BY 1, 2, 3, 4
), open_slots AS (
  SELECT DISTINCT s.country_code
    , s.city_id
    , dates.report_date
    , dates.start_datetime AS start_datetime_local
    , SUM(s.assigned_shifts) + SUM(s.unassigned_shifts) AS open_slots_size
  FROM report_dates dates
  LEFT JOIN il.staffing s ON dates.report_date = CAST(DATETIME(s.start_time, s.timezone) AS DATE)
    AND CAST(DATETIME(s.start_time, s.timezone) AS TIMESTAMP) < dates.end_datetime
    AND CAST(DATETIME(s.end_time, s.timezone) AS TIMESTAMP) > dates.start_datetime
  GROUP BY 1, 2, 3, 4
), manual_staffing AS (
  SELECT DISTINCT o.country_code
    , o.city_id
    , o.report_date
    , o.start_datetime_local
    , open_slots_size
    , manual_assigned AS manual_slots_size
  FROM open_slots o
  LEFT JOIN manual_slots m USING(country_code, city_id, report_date, start_datetime_local)
), latest_forecast AS (
  SELECT DISTINCT a.country_code
    , a.city_id
    , a.zone_external_id
    , a.starting_point_external_id
    , DATETIME(a.demand_for, a.timezone) AS demand_for_local
    , DATETIME(a.created_at, a.timezone) as created_at_local
    , a.forecast_for
    , a.job_run_id
    , a.riders_needed_distributed
  FROM (
    SELECT d.country_code
      , co.city_id
      , d.zone_external_id
      , d.starting_point_external_id
      , ROW_NUMBER() OVER (PARTITION BY d.country_code, city_external_id, starting_point_external_id, demand_for ORDER BY d.created_at DESC) AS rank
      , FIRST_VALUE(d.job_run_id) OVER (PARTITION BY d.country_code, city_external_id, starting_point_external_id, demand_for ORDER BY d.created_at DESC) AS latest_job_run_id
      , co.timezone
      , d.job_run_id
      , d.forecast_for
      , CAST(d.riders_needed_distributed AS INT64) AS riders_needed_distributed
      , d.demand_for
      , d.created_at
    FROM ml.forecast_demands d
    LEFT JOIN countries co ON d.country_code = co.country_code
      AND d.city_external_id = co.city_id
      AND d.zone_external_id = co.zone_id
  ) a
  WHERE rank = 1
    AND a.job_run_id = latest_job_run_id
), rider_demand AS (
  SELECT bf.country_code as country_code
    , bf.city_id as city_id
    , CAST(d.report_date AS DATE) AS report_date
    , d.start_datetime AS start_datetime_local
    , SUM(bf.riders_needed_distributed) AS riders_needed_distributed
  FROM report_dates d
  LEFT JOIN latest_forecast bf ON d.report_date = CAST(bf.demand_for_local AS DATE)
    AND CAST(d.start_datetime AS TIME) = CAST(bf.demand_for_local AS TIME)
  WHERE d.report_date BETWEEN (SELECT start_time FROM parameters) AND (SELECT end_time FROM parameters)
  GROUP BY 1, 2, 3, 3, 4
), first_aggregation AS (
  SELECT country_code
    , city_id
    , report_date_local
    , start_datetime_local
    , IF(rider_demand_fulfillment <= 2, 1- ABS(rider_demand_fulfillment - 1), 0) AS rider_demand_fulfillment
    , manual_shifts
  FROM (
    SELECT m.country_code
      , m.city_id
      , m.report_date AS report_date_local
      , m.start_datetime_local
      , COALESCE(SAFE_DIVIDE(m.manual_slots_size, m.open_slots_size), 0) AS manual_shifts
      , COALESCE(SAFE_DIVIDE(m.open_slots_size, r.riders_needed_distributed), 0) AS rider_demand_fulfillment
    FROM manual_staffing m
    LEFT JOIN rider_demand r ON m.country_code = r.country_code
      AND m.city_id = r.city_id
      AND m.start_datetime_local = r.start_datetime_local
  )
------------------------------ UTR --------------------------------
), utr AS (
  SELECT country_code
    , d.city_id
    , COALESCE(DATE(DATETIME(d.rider_dropped_off_at, o.timezone)), DATE(DATETIME(d.created_at, o.timezone))) AS report_date
    , IF(d.delivery_status = 'completed', o.order_id, NULL) AS order_completed
    , NULL AS working_hours
  FROM cl.orders o
  LEFT JOIN UNNEST(deliveries) d
  WHERE o.created_date >= (SELECT start_time FROM parameters)

  UNION ALL

  SELECT country_code
    , city_id
    , e.day AS report_date
    , NULL AS order_completed
    , (e.duration / 3600) AS working_hours
  FROM cl.shifts s
  LEFT JOIN UNNEST(s.actual_working_time_by_date) e
  WHERE s.created_date >= (SELECT start_time FROM parameters)
), utr_agg AS (
  SELECT country_code
    , city_id
    , report_date AS report_date_local
    , COUNT(DISTINCT order_completed) AS orders_completed
    , SUM(working_hours) AS working_hours
  FROM utr
  GROUP BY 1, 2, 3
)
-------------------------------- FINAL AGGREGATION --------------------------------
SELECT u.country_code
  , u.city_id
  , u.report_date_local
  , SAFE_DIVIDE(orders_completed, working_hours) AS utr
  , AVG(f.rider_demand_fulfillment) AS perc_rider_demand_fulfilled
  , AVG(f.manual_shifts) AS perc_manual_staffing
FROM utr_agg u
LEFT JOIN first_aggregation f USING(country_code, city_id, report_date_local)
GROUP BY 1, 2, 3, 4
;
