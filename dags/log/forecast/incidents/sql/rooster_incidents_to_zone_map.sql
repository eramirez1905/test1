CREATE OR REPLACE TABLE incident_forecast.rooster_incidents_to_zones_map AS
-- only information on production environments is imported from pandora
-- the mapping for testing environments is static and governed by Logistics Data Science
WITH pandora_imports AS (
    SELECT
      environment,
      city_id,
      city_name,
      zone_id,
      zone_name,
      incident_type,
      country_iso,
      language,
      brand
    FROM dl.pandora_rooster_incidents_teams_map
    WHERE
      environment IS NOT NULL AND
      city_id IS NOT NULL AND
      zone_id IS NOT NULL AND
      incident_type IS NOT NULL AND
      country_iso IS NOT NULL AND
      language IS NOT NULL
), testing_environments AS (
    SELECT
      environment,
      city_id,
      city_name,
      zone_id,
      zone_name,
      incident_type,
      country_iso,
      language,
      brand
    FROM incident_forecast.rooster_incidents_to_zones_map_testing_environments
)
SELECT * FROM pandora_imports
UNION ALL
SELECT * FROM testing_environments
