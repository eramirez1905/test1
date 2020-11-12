CREATE OR REPLACE TABLE rl.scorecard_final_dataset_zones AS
SELECT r.country_code
  , r.city_id
  , r.zone_id
  , r.report_week_local
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
FROM rl.scorecard_rider_compliance_zones r
LEFT JOIN rl.scorecard_vendor_compliance_zones v USING(country_code, city_id, zone_id, report_week_local)
LEFT JOIN rl.scorecard_dispatching_zones d USING(country_code, city_id, zone_id, report_week_local)
LEFT JOIN rl.scorecard_infra_zones i USING(country_code, city_id, zone_id, report_week_local)
LEFT JOIN rl.scorecard_staffing_zones s USING(country_code, city_id, zone_id, report_week_local)
;
