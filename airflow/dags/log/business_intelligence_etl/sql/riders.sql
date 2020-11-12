CREATE OR REPLACE TABLE il.riders AS
WITH riders_dataset AS (
SELECT e.country_code
  , e.id AS rider_id
  , e.email AS email_address
  , e.name AS first_name
  , email AS username
  , e.phone_number
  , TRUE AS is_active
  , TRUE AS rider_flag
  , FALSE AS third_party_flag
  , e.created_at
  , ARRAY_AGG(STRUCT(ec.city_id, ec.contract_id, ec.start_at, ec.end_at, ec.status, c.name, c.type) ORDER BY start_at DESC) AS contracts
FROM `{{ params.project_id }}.dl.rooster_employee` e
LEFT JOIN `{{ params.project_id }}.dl.rooster_employee_contract` ec ON ec.country_code = e.country_code
  AND e.id = ec.employee_id
LEFT JOIN `{{ params.project_id }}.dl.rooster_contract` c ON c.country_code = ec.country_code
  AND c.id = ec.contract_id
GROUP BY 1,2,3,4,5,6,7,8,9,10
)
SELECT *
  , (SELECT name FROM UNNEST(contracts) WHERE status = 'VALID' AND CAST(start_at AS DATE) <= '{{ next_ds }}' ORDER BY end_at DESC LIMIT 1) AS latest_contract
  , (SELECT type FROM UNNEST(contracts) WHERE status = 'VALID' AND CAST(start_at AS DATE) <= '{{ next_ds }}' ORDER BY end_at DESC LIMIT 1) AS latest_contract_type
FROM riders_dataset
;
