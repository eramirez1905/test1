INSERT INTO incident_forecast.forecasts_country_iso_language
WITH latest_forecast_run AS (
  SELECT
    country_iso,
    brand,
    incident_type,
    MAX(created_at) AS latest_run
  FROM incident_forecast.forecasts_country_iso
  GROUP BY 1, 2, 3
),
latest_forecasts AS (
-- making sure only to select only the latest forecast per country-brand-incident_type-combination
    SELECT
      datetime,
      hour,
      country_iso,
      brand,
      contact_center,
      dispatch_center,
      CASE WHEN incident_type LIKE 'cs%' OR incident_type LIKE 'ps%'
        THEN timezone_cs_ps ELSE timezone_dp END AS timezone,
      incident_type,
      incidents,
      created_at,
      r.latest_run,
      ROW_NUMBER() OVER (PARTITION BY country_iso, brand, incident_type, datetime ORDER BY created_at DESC) AS _row_number
    FROM incident_forecast.forecasts_country_iso
    LEFT JOIN latest_forecast_run r
      USING (country_iso, brand, incident_type)
), country_language_forecasts AS (
-- splitting country-forecasts into their respective languages
    SELECT
      datetime,
      hour,
      country_iso,
      brand,
      contact_center,
      dispatch_center,
      timezone,
      incident_type,
      language,
      (incidents * percentage) AS incidents,
      -- Since this step effectively creates new forecasts (by splitting existing
      -- forecasts into languages), overwrite the created_at column with the current time.
      -- This column is used when mapping forecasts to Zones to select only the latest forecast.
      CURRENT_DATETIME() AS created_at
    FROM latest_forecasts l
    INNER JOIN incident_forecast.country_incident_language_percentage
     USING(country_iso, brand, incident_type, hour)
    WHERE 
      l.created_at = l.latest_run AND
      -- mainly a precaution, but re-triggering the import of forecasts from S3 into BigQuery
      -- without re-triggering the actual forecasting beforehand can lead to duplicates
      l._row_number = 1
)
SELECT 
  datetime,
  country_iso,
  language,
  brand,
  contact_center,
  dispatch_center,
  timezone,
  incident_type,
  incidents,
  created_at
FROM country_language_forecasts 
ORDER BY country_iso, brand, incident_type, datetime, language
