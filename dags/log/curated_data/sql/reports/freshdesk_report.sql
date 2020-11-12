CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.freshdesk_report`
PARTITION BY created_date AS
SELECT created_date
  , FORMAT_DATE("%G-%V", created_date) AS report_week
  , country_name
  , type
  , source
  , agent
  , survey_result
  , resolution_status
  , status
  , first_response_status
FROM `{{ params.project_id }}.cl._freshdesk`
WHERE created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 8 WEEK)
