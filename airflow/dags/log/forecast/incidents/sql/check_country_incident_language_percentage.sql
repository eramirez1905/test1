-- make sure the percentages per language and country, brand, incident_type and hour
-- sum up to 1, or to zero (if there are no occurrences)
WITH summed_percentages AS (
    SELECT
      country_iso,
      brand,
      incident_type,
      hour,
      CASE WHEN ABS(1 - SUM(percentage)) < 0.01 OR SUM(percentage) = 0 THEN 1 ELSE 0 END AS is_valid_sum
    FROM incident_forecast.country_incident_language_percentage
    GROUP BY 1, 2, 3, 4
)
SELECT MIN(is_valid_sum) = 1 AS is_valid_sum_all
FROM summed_percentages
