CREATE OR REPLACE TABLE rl.scorecard_final_dataset AS
SELECT r.country_code
  , r.city_id
  , r.report_week_local
  , r.completed_deliveries AS count_deliveries
  , perc_reactions_over_2
  , perc_no_show_shifts
  , perc_full_work_shifts
  , perc_vendor_time_over_7
  , perc_customer_time_over_5
  , acceptance_issues
  , avg_issue_solving_time
  , perc_manually_touched_orders
  , estimated_prep_duration
  , cancellation_rate
  , reliability_rate
  , vendor_density
  , order_density
  , pickup_distance
  , dropoff_distance
  , dropoff_accuracy
  , pickup_accuracy
  , perc_rider_demand_fulfilled
  , perc_manual_staffing
FROM rl.scorecard_rider_compliance r
LEFT JOIN rl.scorecard_vendor_compliance v USING(country_code, city_id, report_week_local)
LEFT JOIN rl.scorecard_dispatching d USING(country_code, city_id, report_week_local)
LEFT JOIN rl.scorecard_infra i USING(country_code, city_id, report_week_local)
LEFT JOIN rl.scorecard_staffing s USING(country_code, city_id, report_week_local)
;
