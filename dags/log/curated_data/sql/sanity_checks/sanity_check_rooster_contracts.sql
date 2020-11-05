WITH duplicated_rooster_contract AS (
  SELECT region, country_code, id,  COUNT(*) AS c
  FROM `{{ params.project_id }}.dl.rooster_employee_contract`
  GROUP BY 1,2,3
  HAVING C > 1
)
SELECT count(*) = 0 AS duplicated_contract
from duplicated_rooster_contract
