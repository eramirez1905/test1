CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.riders_pii` AS
WITH employees AS (
  SELECT *
  FROM `{{ params.project_id }}.cl._rooster_employees`
)
SELECT e.country_code
    , e.rider_id
    , e.rider_name
    , e.email
    , e.phone_number
    , e.created_at
FROM employees e
