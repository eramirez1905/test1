CREATE OR REPLACE TABLE incident_forecast.incidents_history_zones AS
-- Map historical incidents to the correct Rooster environment and zone
-- based on the country, language, brand and incident_type.
SELECT
  i.country_iso,
  i.language,
  i.brand,
  i.incident_type,
  i.datetime_local AS datetime,
  i.timezone_local AS timezone,
  i.incidents,
  m.environment,
  m.city_id,
  m.zone_id
FROM incident_forecast.filtered_cc_incidents i
LEFT JOIN incident_forecast.rooster_incidents_to_zones_map m
  USING (country_iso, language, brand, incident_type)
WHERE DATE(datetime_local) <= '{{ ds }}'
