CREATE TEMPORARY FUNCTION latest_forecast() AS
(
 (SELECT MAX(created_at) FROM incident_forecast.forecasts_country_iso_language)
);

INSERT INTO incident_forecast.forecasts_zone
WITH mapped_forecasts AS (
-- assign the correct environment and zone to each forecast
-- depending on country_iso, language, brand and incident_type
    SELECT
      f.datetime,
      f.created_at,
      f.country_iso,
      f.language,
      f.brand,
      f.incident_type,
      f.timezone,
      m.environment,
      m.city_id,
      m.zone_id,
      f.incidents
    FROM incident_forecast.forecasts_country_iso_language f
    LEFT JOIN incident_forecast.rooster_incidents_to_zones_map m
     USING (country_iso, language, brand, incident_type)
    WHERE
     created_at = latest_forecast()
)
SELECT
  environment,
  city_id,
  zone_id,
  timezone,
  incident_type,
  datetime,
  SUM(incidents) AS incidents,
  created_at
FROM mapped_forecasts
WHERE
 environment IS NOT NULL AND
 city_id IS NOT NULL AND
 zone_id IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6, 8
