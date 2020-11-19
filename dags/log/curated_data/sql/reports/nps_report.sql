CREATE OR REPLACE VIEW `{{ params.project_id }}.rl.nps_report` AS
WITH dataset AS(
  SELECT survey_id
    , region
    , country_code
    , country_name
    , city_id
    , city_name
    , vendor_id
    , vendor_name
    , entity_id
    , entity_display_name
    , DATE(survey_create_datetime, timezone) AS survey_create_date
    , created_date AS order_created_date
    , timezone
    , DATE(survey_start_datetime, timezone) AS report_date
    , FORMAT_DATE('%G-%V', DATE(survey_start_datetime, timezone)) as report_week
    , nps_order_id
    , platform_order_code
    , response_id
    , delivery_type
    , nps_score
    , customer_type AS respondent_type
    , nps_reason
    , is_finished
    , is_delivery_experience
    , delivery_status
    , stacked_deliveries
    , assumed_actual_preparation_time_mins AS assumed_actual_preparation_time_mins
    , actual_delivery_time_mins
    , promised_delivery_time_mins
    , order_delay_mins
    , dropoff_distance_manhattan_kms
  FROM `{{ params.project_id }}.cl._nps_survey_response`
  -- removing darkstore pickers to not impact utr as they do not have orders, only working hours
  WHERE country_code NOT LIKE '%dp%'
)
SELECT survey_id
  , region
  , country_code
  , country_name
  , city_id
  , city_name
  , vendor_id
  , vendor_name
  , entity_id
  , entity_display_name
  , survey_create_date
  , order_created_date
  , report_date
  , report_week
  , nps_order_id
  , platform_order_code
  , response_id
  , delivery_type
  , nps_score
  , respondent_type
  , nps_reason
  , is_finished
  , is_delivery_experience
  , delivery_status
  , stacked_deliveries
  , assumed_actual_preparation_time_mins
  , actual_delivery_time_mins
  , promised_delivery_time_mins
  , order_delay_mins
  , dropoff_distance_manhattan_kms
FROM dataset d
WHERE report_date >= DATE_SUB('{{ next_ds }}', INTERVAL 3 MONTH)
  AND report_date < '{{ next_ds }}'
