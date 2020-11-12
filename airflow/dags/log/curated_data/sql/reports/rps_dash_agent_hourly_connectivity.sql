CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.rps_dash_agent_hourly_connectivity`
PARTITION BY created_date_local
CLUSTER BY display_name, country_name, platform_vendor_code AS
WITH entities AS (
  SELECT LOWER(c.country_iso) AS country_code
    , c.country_name
    , platform.entity_id
    , platform.display_name
    , platform.brand_id
  FROM `{{ params.project_id }}.cl.entities` c
  CROSS JOIN UNNEST(c.platforms) platform
), soti_devices AS (
  SELECT d.* EXCEPT (_row_number, country_name, region)
    , IF(d.country_name = 'Hongkong', 'hk', en.country_code) AS country_code
  FROM (
    SELECT region
      , created_date
      , SPLIT(path, '\\')[SAFE_OFFSET(3)] AS country_name
      , HardwareSerialNumber AS serial_number
      , SPLIT(devicename, '_')[SAFE_OFFSET(2)] AS serial_number_secondary
      , NetworkConnectionType
      , ROW_NUMBER() OVER (PARTITION BY HardwareSerialNumber ORDER BY created_at DESC) AS _row_number
    FROM `{{ params.project_id }}.dl.soti_devices`
    WHERE created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 7 DAY)
  ) d
  LEFT JOIN entities en ON d.country_name = en.country_name
  WHERE _row_number = 1
), rps_connectivity_data AS (
  SELECT CAST(s_loc.starts_at AS DATE) AS created_date_local
    , clc.region
    , clc.entity_id
    , clc.country_code
    , clc.vendor_code
    , clc.timezone
    , clc._operator_code
    , clc.vendor_id
    , clc.is_monitor_enabled
    , CAST(s_loc.starts_at AS DATETIME) AS slot_start_at_local
    , CAST(s_loc.ends_at AS DATETIME) AS slot_end_at_local
    , s_loc.duration AS slot_duration
    , s_loc.daily_quantity AS slots
    , s_loc.connectivity.is_unreachable
    , CAST(s_loc.connectivity.unreachable_starts_at AS DATETIME) AS unreachable_start_at_local
    , CAST(s_loc.connectivity.unreachable_ends_at AS DATETIME) AS unreachable_end_at_local
    , s_loc.connectivity.unreachable_duration
    , s_loc.availability.is_offline
    , s_loc.availability.offline_reason
    , CAST(s_loc.availability.offline_starts_at AS DATETIME) AS offline_start_at_local
    , CAST(s_loc.availability.offline_ends_at AS DATETIME) AS offline_end_at_local
    , s_loc.availability.offline_duration
  FROM `{{ params.project_id }}.cl.rps_connectivity` clc
  LEFT JOIN UNNEST(slots_local) s_loc
  WHERE CAST(s_loc.starts_at AS DATE) >= DATE_SUB('{{ next_ds }}', INTERVAL 29 DAY)
    AND clc.country_code != 'de'
), device_events as(
  SELECT *
  FROM(
    SELECT d.entity_id
      , d.vendor_code
      , (SELECT type FROM UNNEST(network_info) ORDER BY created_at LIMIT 1 ) AS type
      , ROW_NUMBER() OVER (PARTITION BY entity_id, vendor_code ORDER BY created_at DESC) AS _row_number
    FROM `{{ params.project_id }}.cl.rps_device_events` d
    WHERE d.created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 7 DAY)
      AND ARRAY_LENGTH(network_info) >= 1
      AND entity_id IS NOT NULL
      AND vendor_code IS NOT NULL
    )
  WHERE _row_number=1
), rps_devices AS(
  SELECT DISTINCT d.*
    , COALESCE(e.type, s.NetworkConnectionType, ss.NetworkConnectionType) AS connection_type
  FROM(
    SELECT DISTINCT entity_id
      , vendor_code
      , country_code
      , mdm_service
      , serial_number
      , client.wrapper_type AS client_wrapper_type
      , client.name AS client_name
      , hardware.manufacturer AS device_manufacturer
      , hardware.model AS device_model
    FROM `{{ params.project_id }}.cl.rps_devices` d
    WHERE d.is_active
      AND d.is_the_latest_version) d
  LEFT JOIN device_events e ON d.entity_id = e.entity_id
    AND d.vendor_code = e.vendor_code
  LEFT JOIN soti_devices s ON s.serial_number = d.serial_number
    AND s.country_code = d.country_code
  LEFT JOIN soti_devices ss ON ss.serial_number_secondary = d.serial_number
    AND ss.country_code = d.country_code
), unique_vendors_with_entity_id AS (
  SELECT * FROM (
    SELECT DISTINCT entity_id
      , country_code
      , vendor_code
      , timezone
      , ROW_NUMBER() OVER (PARTITION BY entity_id, country_code, vendor_code ORDER by created_date_local DESC) AS _row_number
    FROM rps_connectivity_data
  )
  WHERE _row_number = 1
), generated_ts AS (
  SELECT CAST(timestamps AS DATETIME) AS datetimes
    , DATETIME(timestamps, timezone) AS datetimes_local
    , uve.entity_id
    , uve.country_code
    , uve.vendor_code
    , uve.timezone
  FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP, DAY), INTERVAL -29 DAY), current_timestamp, INTERVAL 1 hour)
    ) timestamps
  CROSS JOIN unique_vendors_with_entity_id uve
), table_with_calculations as (
  SELECT DISTINCT CAST(t.datetimes AS DATE) AS created_date
    , slot.region
    , FORMAT_DATETIME('%H:00', t.datetimes) AS timestamps_bin
    , CAST(t.datetimes_local AS DATE) AS created_date_local
    , FORMAT_DATETIME('%H:00', t.datetimes_local) AS timestamps_bin_local
    , t.vendor_code
    , t.entity_id
    , CAST(NULL AS STRING) AS vendor_id
    , CAST(NULL AS STRING) AS operator_code
    , en.display_name
    , en.country_name
    , t.timezone
    , IF(DATETIME_DIFF(slot.slot_end_at_local, t.datetimes_local, MINUTE) > 60,
          60,
          DATETIME_DIFF(slot.slot_end_at_local, t.datetimes_local, MINUTE)
        ) AS slot_mins_until_end
    , IF(DATETIME_DIFF(slot.slot_start_at_local, t.datetimes_local, MINUTE) < 0,
          0,
          DATETIME_DIFF(slot.slot_start_at_local, t.datetimes_local, MINUTE)
        ) AS slot_minutes_from_start
    , unreachability.unreachable_start_at_local
    , IF(DATETIME_DIFF(unreachability.unreachable_end_at_local, t.datetimes_local, MINUTE) > 60,
          60,
          DATETIME_DIFF(unreachability.unreachable_end_at_local, t.datetimes_local, MINUTE)
        ) AS un_mins_until_end
    , IF(DATETIME_DIFF(unreachability.unreachable_start_at_local, t.datetimes_local, MINUTE) < 0,
          0,
          DATETIME_DIFF(unreachability.unreachable_start_at_local, t.datetimes_local, MINUTE)
        ) AS un_minutes_from_start
    , availability.offline_start_at_local
    , IF(DATETIME_DIFF(availability.offline_end_at_local, t.datetimes_local, MINUTE) > 60,
          60,
          DATETIME_DIFF(availability.offline_end_at_local, t.datetimes_local, MINUTE)
        ) AS av_mins_until_end
    , IF(DATETIME_DIFF(availability.offline_start_at_local, t.datetimes_local, MINUTE) < 0,
          0,
          DATETIME_DIFF(availability.offline_start_at_local, t.datetimes_local, MINUTE)
        ) AS av_minutes_from_start
  FROM generated_ts t
  LEFT JOIN entities en on t.entity_id = en.entity_id
  LEFT JOIN rps_connectivity_data slot ON DATE(t.datetimes_local) = slot.created_date_local
    AND t.datetimes_local BETWEEN DATETIME_TRUNC(slot.slot_start_at_local, HOUR)
    AND DATETIME_TRUNC(slot.slot_end_at_local, HOUR)
    AND t.entity_id = slot.entity_id
    AND t.vendor_code = slot.vendor_code
  LEFT JOIN rps_connectivity_data unreachability ON DATE(t.datetimes_local) = unreachability.created_date_local
    AND t.datetimes_local BETWEEN DATETIME_TRUNC(unreachability.unreachable_start_at_local, HOUR)
    AND DATETIME_TRUNC(unreachability.unreachable_end_at_local, HOUR)
    AND t.entity_id = unreachability.entity_id
    AND t.vendor_code = unreachability.vendor_code
  LEFT JOIN rps_connectivity_data availability ON DATE(t.datetimes_local) = availability.created_date_local
    AND t.datetimes_local BETWEEN DATETIME_TRUNC(availability.offline_start_at_local, HOUR)
    AND DATETIME_TRUNC(availability.offline_end_at_local, HOUR)
    AND t.entity_id = availability.entity_id
    AND t.vendor_code = availability.vendor_code
), unreachability_deduplicated AS (
  SELECT DISTINCT created_date
    , region
    , timestamps_bin
    , created_date_local
    , timestamps_bin_local
    , vendor_code
    , entity_id
    , vendor_id
    , operator_code
    , display_name
    , country_name
    , timezone
    , slot_mins_until_end
    , slot_minutes_from_start
    , unreachable_start_at_local
    , un_mins_until_end
    , un_minutes_from_start
  FROM table_with_calculations
), availability_deduplicated AS (
  SELECT DISTINCT created_date
    , region
    , timestamps_bin
    , created_date_local
    , timestamps_bin_local
    , vendor_code
    , entity_id
    , vendor_id
    , operator_code
    , display_name
    , country_name
    , timezone
    , slot_mins_until_end
    , slot_minutes_from_start
    , offline_start_at_local
    , av_mins_until_end
    , av_minutes_from_start
  FROM table_with_calculations
), unreachability_duration AS (
  SELECT DISTINCT created_date
    , region
    , timestamps_bin
    , created_date_local
    , timestamps_bin_local
    , operator_code
    , vendor_id
    , vendor_code
    , CONCAT(entity_id, country_name, vendor_code) AS unique_vendor_id
    , entity_id
    , display_name
    , country_name
    , timezone
    , LEAST(SUM(slot_mins_until_end - slot_minutes_from_start), 60) AS slot_duration
    , CASE
        WHEN LEAST(SUM(un_mins_until_end - un_minutes_from_start), 60) > LEAST(SUM(slot_mins_until_end - slot_minutes_from_start), 60)
          THEN LEAST(SUM(slot_mins_until_end - slot_minutes_from_start), 60)
        ELSE LEAST(SUM(un_mins_until_end - un_minutes_from_start), 60)
      END AS unreachable_duration
    , COUNTIF(un_mins_until_end IS NOT NULL AND un_mins_until_end != 0) AS is_unreachable_ct
  FROM unreachability_deduplicated
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
), availability_duration AS (
  SELECT DISTINCT created_date
    , region
    , timestamps_bin
    , created_date_local
    , timestamps_bin_local
    , operator_code
    , vendor_id
    , vendor_code
    , CONCAT(entity_id, country_name, vendor_code) AS unique_vendor_id
    , entity_id
    , display_name
    , country_name
    , timezone
    , LEAST(SUM(slot_mins_until_end - slot_minutes_from_start), 60) AS slot_duration
    , LEAST(SUM(av_mins_until_end - av_minutes_from_start), 60) AS offline_duration
    , COUNTIF(av_mins_until_end IS NOT NULL AND av_mins_until_end != 0) AS is_offline_ct
  FROM availability_deduplicated
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
), main_date_cleaned AS(
  SELECT COALESCE(un.created_date, av.created_date) AS created_date
    , COALESCE(un.region, av.region) AS region
    , COALESCE(un.timestamps_bin, av.timestamps_bin) AS timestamps_bin
    , COALESCE(un.created_date_local, av.created_date_local) AS created_date_local
    , COALESCE(un.timestamps_bin_local, av.timestamps_bin_local) AS timestamps_bin_local
    , COALESCE(un.operator_code, av.operator_code) AS operator_code
    , COALESCE(un.vendor_id, av.vendor_id) AS vendor_id
    , COALESCE(un.vendor_code, av.vendor_code) AS platform_vendor_code
    , COALESCE(un.unique_vendor_id, av.unique_vendor_id) AS unique_vendor_id
    , COALESCE(un.entity_id, av.entity_id) AS entity_id
    , COALESCE(un.display_name, av.display_name) AS display_name
    , COALESCE(un.country_name, av.country_name) AS country_name
    , COALESCE(un.timezone, av.timezone) AS timezone
    , COALESCE(un.slot_duration, av.slot_duration) AS slot_duration
    , un.unreachable_duration
    , av.offline_duration
    , un.is_unreachable_ct
    , av.is_offline_ct
  FROM unreachability_duration un
  LEFT JOIN availability_duration av ON un.created_date = av.created_date
    AND un.timestamps_bin = av.timestamps_bin
    AND un.vendor_code = av.vendor_code
    AND un.entity_id = av.entity_id
  WHERE COALESCE(un.slot_duration, av.slot_duration) IS NOT NULL
), vendor_status AS(
  SELECT * EXCEPT (_rn)
    , CASE WHEN active is false or test_vendor THEN 0 ELSE 1 END AS is_active
  FROM (
    SELECT DISTINCT v.content.global_entity_id
       , v.content.vendor_id
       , v.content.active
       , v.content.test_vendor
       , ROW_NUMBER() OVER (PARTITION BY v.content.global_entity_id, v.content.vendor_id ORDER BY v.timestamp DESC) AS _rn
     FROM `{{ params.project_id }}.dl.data_fridge_vendor_stream` v
  )
   WHERE _rn = 1
)
SELECT m.created_date
  , m.region
  , m.timestamps_bin
  , m.created_date_local
  , m.timestamps_bin_local
  , m.operator_code
  , m.vendor_id
  , m.platform_vendor_code
  , IF(is_active IS NULL,0,  is_active ) AS is_active
  , m.unique_vendor_id
  , m.entity_id
  , m.display_name
  , m.country_name
  , d.mdm_service
  , d.serial_number
  , d.device_manufacturer
  , d.device_model
  , d.client_wrapper_type
  , d.client_name
  , d.connection_type
  , m.timezone
  , m.slot_duration
  , m.unreachable_duration
  , m.offline_duration
  , m.is_unreachable_ct
  , m.is_offline_ct
  , IF(COALESCE((SUM(m.unreachable_duration) OVER (PARTITION BY m.unique_vendor_id, m.created_Date_local) /
                  NULLIF(SUM(m.slot_duration) OVER (PARTITION BY m.unique_vendor_id, m.created_Date_local)
                         , 0))
               , 0) >= 0.90
        AND SUM(m.offline_duration) OVER (PARTITION BY m.unique_vendor_id, m.created_Date_local) IS NOT NULL
       , m.unique_vendor_id
       , NULL
  ) AS excessive_unreachable_vendors
FROM main_date_cleaned m
LEFT JOIN rps_devices d ON m.platform_vendor_code = d.vendor_code
  AND m.entity_id = d.entity_id
LEFT JOIN vendor_status v ON m.platform_vendor_code = v.vendor_id
  AND m.entity_id = v.global_entity_id
