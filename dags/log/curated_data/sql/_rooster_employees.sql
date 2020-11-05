CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._rooster_employees` AS
WITH employee_field_with_history AS (
  SELECT ef.country_code
    , ef.employee_id
    , ef.name
    , ef.value
    , ef.type
    , ef.created_at
    , ef.updated_at
    , ef.created_date
    , efh.employee_field_name
    , efh.new_value
    , efh.id
    , efh.created_at AS history_created_at
  FROM `{{ params.project_id }}.dl.rooster_employee_field` ef
  LEFT JOIN `{{ params.project_id }}.dl.rooster_employee_field_history` efh ON ef.country_code = efh.country_code
    AND ef.name = efh. employee_field_name
    AND ef.employee_id = efh.employee_id
    AND efh.new_value IS NOT NULL
), employee_field_latest AS (
  SELECT * EXCEPT(_row_number)
  FROM (
    SELECT * EXCEPT(value)
      , IF(id IS NULL, value, new_value) AS value
      , ROW_NUMBER() OVER (PARTITION BY country_code, name, employee_id ORDER BY history_created_at DESC) AS _row_number
    FROM employee_field_with_history
  )
  WHERE _row_number = 1
), employee_field AS (
  SELECT ef.country_code
    , ef.employee_id
    , MAX(ef.updated_at) AS updated_at
    , ARRAY_AGG(
        STRUCT(ef.type
          , ef.name
          , ef.value
        )
      ) AS custom_fields
  FROM employee_field_latest ef
  GROUP BY 1, 2
)
SELECT e.country_code
  , e.id AS rider_id
  , e.name AS rider_name
  , e.email
  , e.phone_number
  , e.reporting_to
  , e.batch_number
  , e.created_at
  , e.birth_date
  , GREATEST(e.updated_at, IFNULL(ef.updated_at, '0001-01-01')) AS updated_at
  , ef.custom_fields
FROM `{{ params.project_id }}.dl.rooster_employee` e
LEFT JOIN employee_field ef ON e.country_code = ef.country_code
  AND e.id = ef.employee_id
