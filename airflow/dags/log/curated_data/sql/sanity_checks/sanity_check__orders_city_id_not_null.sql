SELECT COUNT(*) = 0 AS city_id_null
FROM `{{ params.project_id }}.cl._orders`
WHERE city_id IS NULL
  AND country_code NOT IN ('au', 'de') -- We exclude orders of countries we do not have business with.
  AND created_date BETWEEN DATE_SUB('{{ next_ds }}', INTERVAL 10 DAY) AND DATE_SUB('{{ next_ds }}', INTERVAL 2 DAY)
