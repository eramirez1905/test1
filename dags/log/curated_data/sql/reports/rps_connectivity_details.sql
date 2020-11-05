CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.rps_connectivity_details`
PARTITION BY created_date_local AS
WITH entities AS (
  SELECT entity_id
    , display_name AS platform
    , brand_id
  FROM `{{ params.project_id }}.cl.entities` c
  LEFT JOIN UNNEST(c.platforms) platform
), countries AS (
  SELECT LOWER(country_iso) AS country_code
    , country_name AS country
  FROM `{{ params.project_id }}.cl.countries`
  GROUP BY 1,2
), connectivity_data AS (
  SELECT CAST(CAST(s_loc.starts_at AS TIMESTAMP) AS DATE) AS created_date_local
    , en.platform
    , co.country
    , clc.vendor_code AS platform_restaurant_id
    , clc._operator_code AS rps_operator_code
    , clc.vendor_id AS rps_restaurant_id
    , DATETIME_TRUNC(s_loc.starts_at, SECOND) AS slot_start_at_local
    , DATETIME_TRUNC(s_loc.ends_at, SECOND) AS slot_end_at_local
    , s_loc.duration AS slot_duration
    , s_loc.daily_schedule_duration
    , s_loc.connectivity.is_unreachable
    , DATETIME_TRUNC(s_loc.connectivity.unreachable_starts_at, SECOND) AS unreachable_start_at_local
    , DATETIME_TRUNC(s_loc.connectivity.unreachable_ends_at, SECOND) AS unreachable_end_at_local
    , s_loc.connectivity.unreachable_duration
    , s_loc.availability.is_offline
    , s_loc.availability.offline_reason
    , DATETIME_TRUNC(s_loc.availability.offline_starts_at, SECOND) AS offline_start_at_local
    , DATETIME_TRUNC(s_loc.availability.offline_ends_at, SECOND) AS offline_end_at_local
    , s_loc.availability.offline_duration
  FROM `{{ params.project_id }}.cl.rps_connectivity` clc
  LEFT JOIN UNNEST(slots_local) s_loc
  LEFT JOIN entities en ON clc.entity_id = en.entity_id
  LEFT JOIN countries co ON clc.country_code = co.country_code
)
SELECT created_date_local
  , platform
  , country
  , platform_restaurant_id
  , rps_operator_code
  , rps_restaurant_id
  , slot_start_at_local
  , slot_end_at_local
  , CAST(slot_start_at_local AS STRING) AS slot_start_at_local_text
  , CAST(slot_end_at_local AS STRING) AS slot_end_at_local_text
  , slot_duration
  , daily_schedule_duration
  , is_unreachable
  , unreachable_start_at_local
  , unreachable_end_at_local
  , CAST(unreachable_start_at_local AS STRING) AS unreachable_start_at_local_text
  , CAST(unreachable_end_at_local AS STRING) AS unreachable_end_at_local_text
  , unreachable_duration
  , (unreachable_start_at_local BETWEEN slot_start_at_local AND DATETIME_ADD(slot_start_at_local, INTERVAL 30 MINUTE)) AS is_unreachable_schedule_start
  , is_offline
  , offline_reason
  , offline_start_at_local
  , offline_end_at_local
  , CAST(offline_start_at_local AS STRING) AS offline_start_at_local_text
  , CAST(offline_end_at_local AS STRING) AS offline_end_at_local_text
  , offline_duration
FROM connectivity_data
