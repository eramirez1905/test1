DELETE FROM `{{ params.project_id }}.raw.{{ params.table_name }}`
WHERE country_code = 'dp_sg'
  AND created_date >= '2020-01-01'
