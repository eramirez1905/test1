CREATE TEMPORARY FUNCTION get_start_date() AS
(
 DATE_SUB('{{ next_ds }}', INTERVAL 28 DAY)
);

CREATE TEMPORARY FUNCTION get_end_date() AS
(
 DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY)
);

CREATE TEMPORARY FUNCTION get_weight(dt DATE) AS
(
 -- observations from the past 7 days get weight 3
 -- observations older than 14 days get weight 1
 -- in between: weight 2
  LEAST(GREATEST(3 - FLOOR(DATE_DIFF('{{ next_ds }}', dt, DAY) / 7), 1), 3)
);

CREATE OR REPLACE TABLE incident_forecast.country_incident_language_percentage AS
WITH
incidents_data AS (
  -- select data on incidents from the desired time frame
    SELECT
      country_iso,
      brand,
      language,
      incident_type,
      hour,
      incidents * get_weight(DATE(datetime_local)) AS weighted_incidents
    FROM incident_forecast.filtered_cc_incidents
      WHERE DATE(datetime_local) BETWEEN get_start_date() AND get_end_date()
),
hours AS (
    -- select all 48 half-hours of the day
    SELECT
      (slot / 2) AS hour
    FROM UNNEST(GENERATE_ARRAY(0, 47)) AS slot
),
grid AS (
    -- select all relevant combinations of country_iso, brand, incident_type, language and hour
    -- we will need a prediction for every one of these combinations
    SELECT DISTINCT
      country_iso,
      brand,
      incident_type,
      language,
      h.hour
    FROM incidents_data
    CROSS JOIN hours h
),
country_brand_hourly_total AS (
    -- get the total number of incidents, independent of the language
    SELECT
      country_iso,
      brand,
      incident_type,
      hour,
      SUM(weighted_incidents) AS weighted_total_incidents_country
    FROM incidents_data
    GROUP BY 1, 2, 3, 4
), country_brand_language_hourly_total AS (
    -- get the total number of incidents, taking language into account
    SELECT
      country_iso,
      brand,
      incident_type,
      language,
      hour,
      SUM(weighted_incidents) AS weighted_total_incidents_language
    FROM incidents_data
    GROUP BY 1, 2, 3, 4, 5
), hourly_level_percentage AS (
    SELECT
      country_iso,
      brand,
      incident_type,
      language,
      hour,
      ROUND((weighted_total_incidents_language / GREATEST(1, weighted_total_incidents_country)), 3) AS hourly_percentage
    FROM country_brand_language_hourly_total
    LEFT JOIN country_brand_hourly_total
     USING (country_iso, brand, incident_type, hour)
)
SELECT
  country_iso,
  brand,
  incident_type,
  language,
  hour,
  COALESCE(hourly_percentage, 0) AS percentage
FROM grid
LEFT JOIN hourly_level_percentage
USING (country_iso, brand, incident_type, language, hour)
ORDER BY 1, 2, 3, 5, 4
