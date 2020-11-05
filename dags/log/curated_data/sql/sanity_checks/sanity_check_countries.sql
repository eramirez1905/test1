WITH duplicated_country_code AS (
  SELECT country_code, count(*) AS c
  FROM `{{ params.project_id }}.cl.countries`
  WHERE country_code IS NOT NULL
  GROUP BY 1
  HAVING C > 1
), duplicated_entities AS (
  SELECT country_code, p.entity_id, count(*) AS c
  FROM `{{ params.project_id }}.cl.countries`
  LEFT JOIN UNNEST(platforms) p
  WHERE country_code IS NOT NULL
  GROUP BY 1,2
  HAVING C > 1
)
SELECT (SELECT count(*) = 0 FROM duplicated_country_code) AS duplicated_country_code
  , (SELECT count(*) = 0 FROM duplicated_entities) AS duplicated_entities
