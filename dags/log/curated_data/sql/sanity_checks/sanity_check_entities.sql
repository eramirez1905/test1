WITH duplicated_country_iso AS (
  SELECT country_iso, COUNT(*) AS c
  FROM `{{ params.project_id }}.cl.entities`
  GROUP BY 1
  HAVING C > 1
)
SELECT count(*) = 0 AS duplcated_country_iso
from duplicated_country_iso
