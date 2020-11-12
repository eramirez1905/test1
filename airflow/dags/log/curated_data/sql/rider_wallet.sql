CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.rider_wallet`
PARTITION BY created_date
CLUSTER BY country_code, rider_id, delivery_id AS
SELECT j.country_code
  , j.id AS transaction_id
  , COALESCE(j.user_id, j.courier_id) AS rider_id
  , j.delivery_id
  , j.order_id
  , o.city_id
  , o.timezone
  , j.created_at
  , j.updated_at
  , CAST(j.created_at AS DATE) AS created_date
  , j.type
  , j.amount
  , j.balance
  , j.created_by
  , j.manual
  , j.mass_update
  , j.note
  , payment_transaction.provider_id AS external_provider_id
  , payment_transaction.integrator_id
  , payment_transaction.status
  , payment_transaction.message
  , ARRAY_AGG(STRUCT(i.id
    , i.original_amount
    , i.order_id
    , i.justification
    , i.created_by
    , i.created_at
    , i.updated_at
    , i.updated_by
    , i.reviewed
    , i.approved
  )) AS issues
FROM `{{ params.project_id }}.dl.cod_journal` j
LEFT JOIN `{{ params.project_id }}.dl.cod_issue` i USING (country_code, order_id)
LEFT JOIN `{{ params.project_id }}.cl.orders` o ON j.country_code = o.country_code
  AND j.order_id = o.order_id
LEFT JOIN `{{ params.project_id }}.dl.cod_payment_transaction` payment_transaction ON j.country_code = payment_transaction.country_code
  AND j.note = payment_transaction.transaction_id
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21
