CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._porygon_vehicle_profile_versions`
PARTITION BY created_date AS
WITH porygon_vehicle_profile_history_calculation AS (
  SELECT id AS vendor_code
    , platform
    , country_code
    , created_date
    , updated_at
    , vehicle_profile
    , LAG(vehicle_profile) OVER (PARTITION BY id, platform, country_code ORDER BY updated_at ASC)
      AS previous_vehicle_profile
  FROM `{{ params.project_id }}.hl.porygon_restaurants`
), porygon_vehicle_profile_active_history AS (
  SELECT vendor_code
    , platform
    , country_code
    , created_date
    , vehicle_profile
    , updated_at
  FROM porygon_vehicle_profile_history_calculation
  WHERE vehicle_profile <> previous_vehicle_profile OR previous_vehicle_profile IS NULL
), porygon_vehicle_profile_timestamps AS (
SELECT vendor_code
    , platform
    , country_code
    , created_date
    , vehicle_profile
    , updated_at
    , LEAD(updated_at) OVER (PARTITION BY country_code, vendor_code ORDER BY country_code, vendor_code, updated_at)
      AS next_updated_at
FROM porygon_vehicle_profile_active_history
)
SELECT vendor_code
  , platform
  , country_code
  , created_date
  , ARRAY_AGG(
      STRUCT(vehicle_profile
        , updated_at AS active_from
        , COALESCE(next_updated_at, '{{next_execution_date}}') AS active_to
    ) ORDER BY updated_at DESC) AS vehicle_profile_history
FROM porygon_vehicle_profile_timestamps
GROUP BY 1, 2, 3, 4
