CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.payments_referral_rules`
PARTITION BY created_date AS
SELECT country_code
  , created_date
  , created_at
  , updated_at
  , start_date
  , end_date
  , id
  , unit_type
  , threshold
  , active
  , signing_bonus
  , signing_bonus_amount
  , amount
  , status
  , created_by
  , duration
  , applies_to
  , ARRAY_AGG(STRUCT(contract_types
     , contract_ids
     , city_ids
     , vehicle_types
     , job_titles
   )) AS details
FROM `{{ params.project_id }}.dl.rooster_payments_referral_payment_rule`
GROUP BY  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
