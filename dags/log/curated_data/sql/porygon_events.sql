CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.porygon_events`
PARTITION BY created_date
CLUSTER BY country_code, event_id, platform AS
WITH countries AS (
  SELECT country_code
    , ci.id AS city_id
    , ci.timezone AS timezone
    , zo.id AS zone_id
  FROM `{{ params.project_id }}.cl.countries` co
  LEFT JOIN UNNEST(co.cities) ci
  LEFT JOIN UNNEST(ci.zones) zo
), countries_timezone AS (
  SELECT c.country_code
    -- taking one timezone per country to use in the final table
    , (SELECT timezone FROM UNNEST (cities) ci LIMIT 1) AS timezone
  FROM `{{ params.project_id }}.cl.countries` c
)
SELECT e.country_code
  , co.city_id
  , COALESCE(co.timezone, ct.timezone) AS timezone
  , e.id AS event_id
  , e.platform
  , e.created_at
  , e.created_date
  , pe.shape_sync as is_shape_in_sync
  , ARRAY_AGG(STRUCT(
    transaction_id
    , end_transaction_id
    , t1.issued_at AS start_at
    , t2.issued_at AS end_at
    , TIMESTAMP_DIFF(t2.issued_at, t1.issued_at, SECOND) AS duration
    , e.title
    , e.message
    , e.tags
    , e.action
    , e.is_active
    , COALESCE(SAFE_CAST(JSON_EXTRACT_SCALAR(e.tags, '$.halal') AS BOOL), FALSE) AS is_halal
    , e.value
    , e.activation_threshold
    , e.deactivation_threshold
    , COALESCE(e.zone_id, 0) AS zone_id
    , SAFE.ST_GEOGFROMTEXT(e.shape_wkt) AS shape
  ) ORDER BY t1.issued_at) AS transactions
FROM `{{ params.project_id }}.dl.porygon_events_versions` e
INNER JOIN `{{ params.project_id }}.dl.porygon_transaction` t1 ON e.country_code = t1.country_code
  AND e.transaction_id = t1.id
INNER JOIN `{{ params.project_id }}.dl.porygon_transaction` t2 ON e.country_code = t2.country_code
  AND e.end_transaction_id = t2.id
LEFT JOIN countries co ON e.country_code = co.country_code
  AND e.zone_id = co.zone_id
LEFT JOIN countries_timezone ct ON e.country_code = ct.country_code
LEFT JOIN `{{ params.project_id }}.dl.porygon_events` pe ON pe.country_code = e.country_code
  AND pe.id = e.id AND pe.platform = e.platform
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
