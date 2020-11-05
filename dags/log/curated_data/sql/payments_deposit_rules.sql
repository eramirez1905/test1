CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.payments_deposit_rules`
PARTITION BY created_date AS
SELECT region
  , country_code
  , created_date
  , id
  , created_by
  , created_at
  , approved_at
  , approved_by
  , updated_at
  , active
  , name
  , description
  , amount
  , cycle_number
  , gross_earning_percentage
  , status
  , ARRAY_AGG(STRUCT(contract_ids
      , city_ids
    )) AS applies_to
FROM `{{ params.project_id }}.dl.rooster_payments_deposit_payment_rule`
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
