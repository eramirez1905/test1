CREATE OR REPLACE TABLE il.audit_log
PARTITION BY created_date AS
{% if not params.full_import -%}
WITH archive AS (
  SELECT *
  FROM il.audit_log
  WHERE created_date < DATE_SUB('{{ next_ds }}', INTERVAL 10 DAY)
)
SELECT * FROM archive UNION ALL
{% endif -%}
SELECT co.country_name AS country_name
  , co.country_code
  , dt.city AS city_name
  , c.city_id
  , dt.action
  , dt.id AS log_id
  , dt.email AS user_email
  , dt.user_id
  , dt.application
  , dt.order_code
  , CAST(JSON_EXTRACT_SCALAR(dt.old_data,'$.status') AS STRING) AS old_status
  , CAST(JSON_EXTRACT_SCALAR(dt.new_data,'$.status') AS STRING) AS new_status
  , CAST(JSON_EXTRACT_SCALAR(dt.new_data, '$.courier_id') AS INT64) AS new_courier_id
  , CAST(JSON_EXTRACT_SCALAR(dt.old_data, '$.courier_id') AS INT64) AS old_courier_id
  , JSON_EXTRACT_SCALAR(dt.new_data, '$.email') AS new_courier_email
  , JSON_EXTRACT_SCALAR(dt.old_data, '$.email') AS old_courier_email
  , CAST(JSON_EXTRACT(old_data, '$.current_long') AS FLOAT64) AS old_long
  , CAST(JSON_EXTRACT(old_data, '$.current_lat') AS FLOAT64) AS old_lat
  , CAST(JSON_EXTRACT(new_data, '$.current_long') AS FLOAT64) AS new_long
  , CAST(JSON_EXTRACT(new_data, '$.current_lat') AS FLOAT64) AS new_lat
  , dt.created_at
  , c.timezone
  , dt.created_date
FROM `{{ params.project_id }}.ml.audit_log_audit_log` dt
LEFT JOIN il.countries co ON co.country_code = dt.country_code
LEFT JOIN il.cities c ON co.country_code = c.country_code
  AND dt.city = c.city_name
WHERE dt.application NOT IN ('iam')
{%- if not params.full_import %}
  AND created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 10 DAY)
{%- endif %}
;
