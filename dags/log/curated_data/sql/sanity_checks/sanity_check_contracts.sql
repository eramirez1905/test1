WITH dataset AS (
  SELECT ec.country_code, ec.contract_id, c.id
  FROM `{{ params.project_id }}.dl.rooster_employee_contract` ec
  LEFT JOIN `{{ params.project_id }}.dl.rooster_contract` c ON ec.country_code = c.country_code
    AND ec.contract_id = c.id
)
SELECT COUNTIF(id IS NULL) = 0 AS contract_id_null_count
FROM dataset
