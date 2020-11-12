CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._rps_mdm_devices`
PARTITION BY created_date
CLUSTER BY service, region AS
WITH entities AS (
  SELECT region_short_name AS region
    , country_name
    , LOWER(country_iso) AS country_code
  FROM `{{ params.project_id }}.cl.entities`
), mdm_soti_devices AS (
  SELECT d.* EXCEPT (_row_number, country_name, region)
    -- This mapping logic is applied as SOTI still have some US/KR devices in EU/AP instances atm
    , IF(d.region = 'eu', en.region, d.region) AS region
    , IF(d.country_name = 'Hongkong', 'hk', en.country_code) AS country_code
  FROM (
    SELECT region
      , 'SOTI' AS service
      , created_date
      , EnrollmentTime AS assigned_at
      , updated_at
      , SPLIT(path, '\\')[SAFE_OFFSET(3)] AS country_name
      , HardwareSerialNumber AS device_id
      , UPPER(HardwareSerialNumber) AS _device_id_for_join
      , HardwareSerialNumber AS serial_number
      , DeviceId AS private_uuid
      , (Mode = 'Active') AS is_active
      , NULL AS vendor_id
      , UPPER(model) AS model
      , UPPER(manufacturer) AS manufacturer
      , 'ANDROID' AS os
      , OSVersion AS os_version
      , Memory.TotalMemory AS total_memory
      , Memory.TotalStorage AS total_storage
      , iccid
      , NetworkBSSID AS bssid
      , CAST(imei_meid_esn AS STRING) AS imei_meid_esn
      , ROW_NUMBER() OVER (PARTITION BY HardwareSerialNumber ORDER BY created_at DESC) AS _row_number
    FROM `{{ params.project_id }}.dl.soti_devices`
  ) d
  LEFT JOIN entities en ON d.country_name = en.country_name
  WHERE _row_number = 1
), mdm_mobilock_devices AS (
  SELECT d.* EXCEPT (_row_number, country_code)
    , en.country_code
    , en.region
  FROM (
    SELECT 'MOBILOCK' AS service
      , created_date
      , COALESCE(location.created_at, last_connected_at) AS assigned_at
      , last_connected_at AS updated_at
      -- This logic is applied in Mobilock as the information of country is scattered into 3 columns in Mobilock
      , LOWER(
          CASE
            WHEN `group`.name IS NOT NULL AND SUBSTR(`group`.name, 0, 7) = 'foodora'
              THEN SUBSTR(`group`.name, 9, 2)
            WHEN `group`.name IS NOT NULL
              THEN SUBSTR(`group`.name, 0, 2)
            WHEN `group`.name IS NULL AND profile.name IS NOT NULL
              THEN SUBSTR(profile.name, 11, 2)
            ELSE CAST(NULL AS STRING)
          END
        ) AS country_code
      , serial_no AS device_id
      , UPPER(serial_no) AS _device_id_for_join
      , serial_no AS serial_number
      , id AS private_uuid
      , CAST(NULL AS BOOL) AS is_active
      , NULL AS vendor_id
      , UPPER(model) AS model
      , UPPER(make) AS manufacturer
      , 'ANDROID' AS os
      , os_version
      , NULL AS total_memory
      , NULL AS total_storage
      , app_version_name AS app_version
      , imei_no AS imei
      , iccid_no AS iccid
      , gsm_serial_no
      , ROW_NUMBER() OVER (PARTITION BY serial_no ORDER BY created_at DESC) AS _row_number
    FROM `{{ params.project_id }}.dl.mobilock_devices`
    WHERE serial_no IS NOT NULL
  ) d
  LEFT JOIN entities en ON d.country_code = en.country_code
  WHERE _row_number = 1
)
SELECT COALESCE(soti.region, mobilock.region) AS region
  , COALESCE(soti.service, mobilock.service) AS service
  , COALESCE(soti.created_date, mobilock.created_date) AS created_date
  , COALESCE(LEAST(soti.assigned_at, mobilock.assigned_at), soti.assigned_at, mobilock.assigned_at) AS assigned_at
  , COALESCE(GREATEST(soti.updated_at, mobilock.updated_at), soti.updated_at, mobilock.updated_at) AS updated_at
  , COALESCE(soti.country_code, mobilock.country_code) AS country_code
  , COALESCE(soti.device_id, mobilock.device_id) AS device_id
  , COALESCE(soti._device_id_for_join, mobilock._device_id_for_join) AS _device_id_for_join
  , COALESCE(soti.private_uuid, mobilock.private_uuid) AS private_uuid
  , COALESCE(soti.serial_number, mobilock.serial_number) AS serial_number
  , COALESCE(soti.is_active, mobilock.is_active, FALSE) AS is_active
  , COALESCE(soti.vendor_id, mobilock.vendor_id) AS vendor_id
  , COALESCE(soti.model, mobilock.model) AS model
  , COALESCE(soti.manufacturer, mobilock.manufacturer) AS manufacturer
  , COALESCE(soti.os, mobilock.os) AS os
  , COALESCE(soti.os_version, mobilock.os_version) AS os_version
  , soti.total_memory
  , soti.total_storage
  , COALESCE(soti.iccid, mobilock.iccid) AS iccid
  , mobilock.gsm_serial_no
  , COALESCE(soti.imei_meid_esn, mobilock.imei) AS imei
  , mobilock.app_version
FROM mdm_soti_devices soti
FULL JOIN mdm_mobilock_devices mobilock ON soti._device_id_for_join = mobilock._device_id_for_join
