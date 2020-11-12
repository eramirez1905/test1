CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.das_scorecard_zones` AS
WITH automated_events AS (
  SELECT DISTINCT country_code
     , t.zone_id
  FROM `{{ params.project_id }}.cl.porygon_events`
  LEFT JOIN UNNEST(transactions) t
  WHERE t.zone_id != 0
  -- removing darkstore pickers to not impact utr as they do not have orders, only working hours
  AND country_code NOT LIKE '%dp%'
)
SELECT c.country_code
  , c.country_name
  , ci.name AS city_name
  , zo.name AS zone_name
  , zo.id
  , zo.has_default_delivery_area_settings
  , zone_id IS NOT NULL AS has_automated_events
FROM `{{ params.project_id }}.cl.countries` c
LEFT JOIN UNNEST(cities) ci
LEFT JOIN UNNEST(zones) zo
LEFT JOIN automated_events a ON c.country_code = a.country_code
  AND a.zone_id = zo.id
WHERE zo.is_active IS TRUE
-- removing darkstore pickers to not impact utr as they do not have orders, only working hours
  AND c.country_code NOT LIKE '%dp%'
