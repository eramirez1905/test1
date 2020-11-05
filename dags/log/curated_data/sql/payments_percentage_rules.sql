CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.payments_percentage_rules`
PARTITION BY created_date AS
SELECT country_code
  , created_date
  , created_at
  , updated_at
  , id
  , name
  , created_by
  , status
  , active
  , start_date
  , end_date
  , percentage
  , apply_to
  , ARRAY_AGG(STRUCT(contract_types
     , job_titles
     , city_ids
   )) AS applies_to
FROM `{{ params.project_id }}.dl.rooster_payments_percentage_payment_rule`
GROUP BY  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
