-- The incidents data is imported from Pandora BI
-- and is already mostly pre-filtered.
-- Per country_iso, only incident_types and languages are included
-- that have recently seen incidents.
CREATE OR REPLACE TABLE incident_forecast.filtered_cc_incidents AS
WITH incidents_data AS (
    SELECT
      country_iso,
      language,
      brand,
      contact_center,
      dispatch_center,
      location_timezone AS timezone_local,
      cs_ps_timezone AS timezone_cs_ps,
      dp_timezone AS timezone_dp,
      DATETIME(start_datetime) AS datetime_local,
      EXTRACT(DAYOFWEEK FROM start_datetime) AS weekday,
      EXTRACT(HOUR FROM start_datetime) + EXTRACT(MINUTE FROM start_datetime) / 60 AS hour,
      incident_type,
      count_incidents AS incidents,
      -- calculate the cumulative sum of incidents, used for filtering
      SUM(count_incidents) OVER( PARTITION BY country_iso, brand, language, incident_type ORDER BY start_datetime) AS incidents_cumsum
    FROM dl.pandora_global_cc_traffic
)
SELECT
  * EXCEPT(incidents_cumsum)
FROM incidents_data
WHERE
  -- Select only desired countries, incident types and time frame
  country_iso in ({{ params.forecast_countries }}) AND
  incident_type in ({{ params.incident_types }}) AND
  DATE(datetime_local) >= DATE_SUB('{{ next_ds }}', INTERVAL 180 DAY) AND
  -- Exclude "non-existing" time slots (appearing due to daylight-saving time)
  datetime_local = DATETIME(TIMESTAMP(datetime_local, timezone_local), timezone_local) AND
  -- Consider only time frames after the 100th incident per country, brand, language, incident_type
  incidents_cumsum > 100
