CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.payment_report_tableau`
PARTITION BY report_date_local AS
SELECT region
  , country_name
  , exchange_rate
  , city_name
  , zone_name
  , country_code
  , rider_id
  , vehicle_name
  , contract_name
  , contract_type
  , report_date_local
  , report_weekday
  , report_week
  , report_month
  , batch_number
  , zone_id
  , city_id
  , deliveries
  , working_hours
  , basic_total
  , basic_per_delivery
  , basic_per_hour
  , per_near_dropoff_deliveries
  , per_picked_up_deliveries
  , per_near_pickup_deliveries
  , per_all_distances
  , per_dropoff_distances
  , per_completed_deliveries
  , quest_total
  , scoring_total
  , total_date_payment
  , pickup_distance
  , dropoff_distance
  , UTR
  , acceptance_rate
FROM `{{ params.project_id }}.rl.payment_report`
WHERE report_date_local >= DATE_SUB('{{ next_ds }}', INTERVAL 8 WEEK)
