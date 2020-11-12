CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.rps_gowin_installation_per_device` AS
WITH gowin_uniqle_installation_per_device AS (
  SELECT DISTINCT region
    , country_code
    , device_id
    , windows_version
    , client_version
    , installation_action
    , (installation_result = 'FAILURE') AS is_result_failure
  FROM `{{ params.project_id }}.cl.rps_vendor_client_installations`
  WHERE client_name  = 'GOWIN'
    AND device_id IS NOT NULL
), gowin_uniqle_installation_per_device_with_installation_profile AS (
  SELECT *
    , (devices_with_success_installation_ct > 0 AND devices_with_failure_installation_ct > 0) AS is_device_with_mixed_installations
  FROM (
    SELECT region
      , country_code
      , device_id
      , windows_version
      , client_version
      , installation_action
      , COUNTIF(NOT is_result_failure) AS devices_with_success_installation_ct
      , COUNTIF(is_result_failure) AS devices_with_failure_installation_ct
    FROM gowin_uniqle_installation_per_device
    GROUP BY 1,2,3,4,5,6
  )
), gowin_uniqle_installation_per_device_with_installation_profile_processed AS (
SELECT * EXCEPT (devices_with_success_installation_ct, devices_with_failure_installation_ct, is_device_with_mixed_installations)
  , IF(NOT is_device_with_mixed_installations AND devices_with_success_installation_ct >= 1, 1, 0) AS devices_with_success_installation_ct
  , IF(NOT is_device_with_mixed_installations AND devices_with_failure_installation_ct >= 1, 1, 0) AS devices_with_failure_installation_ct
  , IF(is_device_with_mixed_installations, 1, 0) AS devices_with_mixed_installation_ct
FROM gowin_uniqle_installation_per_device_with_installation_profile
)
SELECT *
  -- These columns are for visualization sorting only
  , CAST(SPLIT(client_version, '.')[OFFSET(0)] AS INT64) AS _major_version
  , CAST(SPLIT(client_version, '.')[OFFSET(1)] AS INT64) AS _minor_version
  , CAST(SPLIT(client_version, '.')[OFFSET(2)] AS INT64) AS _patch_version
FROM gowin_uniqle_installation_per_device_with_installation_profile_processed
