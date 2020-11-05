CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.payments_basic_rules`
PARTITION BY created_date AS
SELECT country_code
  , created_date
  , created_at
  , updated_at
  , id
  , type
  , sub_type
  , name
  , amount
  , created_by
  , status
  , active
  , start_date
  , end_date
  , start_time
  , end_time
  , acceptance_rate
  , threshold
  , pay_below_threshold AS is_pay_below_threshold
  , max_threshold
  , ARRAY_AGG(STRUCT(contract_types
     , employee_ids AS rider_ids
     , vehicle_types
     , contract_ids
     , job_titles
     , starting_point_ids
     , days_of_week
     , city_ids
     , vertical_types
   )) AS applies_to
FROM `{{ params.project_id }}.dl.rooster_payments_basic_payment_rule`
WHERE active
GROUP BY  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20
