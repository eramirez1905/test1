-- push only the last 6 weeks to rooster
CREATE TEMPORARY FUNCTION get_start_date() AS
(
 DATE_SUB('{{ next_ds }}', INTERVAL 43 DAY)
);

CREATE OR REPLACE TABLE incident_forecast.incidents_history_environment_{{ params.environment_underscore }} AS
SELECT
  environment AS country_code,
  zone_id,
  city_id,
  TIMESTAMP(datetime, timezone) AS datetime,
  SUM(incidents) AS orders,
  -- Both orders and orders_completed needed for History to be displayed in Rooster
  SUM(incidents) AS orders_completed,
FROM incident_forecast.incidents_history_zones
WHERE environment = '{{ params.environment }}'
AND DATE(datetime) BETWEEN get_start_date() AND '{{ ds }}'
GROUP BY 1, 2, 3, 4
