CREATE TEMPORARY FUNCTION latest_forecast() AS
(
 (SELECT MAX(created_at) FROM incident_forecast.forecasts_zone)
);

CREATE OR REPLACE TABLE incident_forecast.forecasts_environment_{{ params.environment_underscore }} AS
SELECT
  environment AS country_code,
  zone_id,
  TIMESTAMP(datetime, timezone) AS datetime,
  -- take the sum of incidents for cases where one zone handles multiple incident types
  SUM(incidents) AS orders
FROM incident_forecast.forecasts_zone
WHERE
 environment = '{{ params.environment }}' AND
 created_at = latest_forecast() AND
 datetime IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY datetime
