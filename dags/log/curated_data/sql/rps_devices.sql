CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.rps_devices`
PARTITION BY created_date
CLUSTER BY region, service, device_id AS
WITH entities AS (
  SELECT en.country_iso
    , platform.entity_id
    , platform.display_name AS entity_display_name
    , platform.brand_id
    , rps_platforms
  FROM `{{ params.project_id }}.cl.entities` en
  LEFT JOIN UNNEST(en.platforms) platform
  LEFT JOIN UNNEST(platform.rps_platforms) rps_platforms
), vendors AS (
  SELECT rps.region
    , v.entity_id
    , rps.contracts.country_code
    , v.vendor_code
    , rps.client.name AS client_name
    , rps.timezone
    , rps.vendor_id
    , rps.operator_code AS _operator_code
    , rps.contracts.delivery_type
    , rps.rps_global_key
    , rps.is_active
  FROM `{{ params.project_id }}.cl.vendors_v2` v
  LEFT JOIN UNNEST(rps) rps
  WHERE rps.is_latest
), mdm_devices AS (
  SELECT *
  FROM `{{ params.project_id }}.cl._rps_mdm_devices`
), dms_devices_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl._rps_dms_devices`
), rps_devices_clean AS (
  SELECT d.* EXCEPT(country_code, manufacturer, client_name, client_version)
    , COALESCE(d.country_code, v.country_code) AS country_code
    -- Fetch manufacturer from model
    , UPPER(COALESCE(d.manufacturer, SUBSTR(model, 0, GREATEST(STRPOS(d.model, ' ') - 1, 0)))) AS manufacturer
    , IF(v.client_name = 'POS', 'POS', COALESCE(d.client_name, v.client_name)) AS client_name
    , IF(v.client_name = 'POS', NULL, d.client_version) AS client_version
    , v.entity_id
    , v.vendor_code
    , COALESCE(serial_number, _device_id_for_join) AS _join_key
  FROM dms_devices_dataset d
  LEFT JOIN vendors v ON d.vendor_id = v.vendor_id
    AND d.region = v.region
), rps_devices_with_mdm AS (
  SELECT COALESCE(rps.region, mdm.region) AS region
    , rps.service
    , IF((rps.mdm_service <> 'SOTI' OR rps.mdm_service IS NULL) AND mdm.service = 'SOTI' AND mdm.device_id IS NOT NULL
        , 'SOTI'
        , COALESCE(rps.mdm_service, mdm.service)
      ) AS mdm_service
    , COALESCE(rps.created_date, mdm.created_date) AS created_date
    , COALESCE(rps.assigned_at, mdm.assigned_at) AS assigned_at
    , COALESCE(rps.updated_at, mdm.updated_at) AS updated_at
    , COALESCE(rps.country_code, mdm.country_code) AS country_code
    , TRIM(COALESCE(rps.device_id, mdm.device_id)) AS device_id
    , COALESCE(rps.serial_number, mdm.serial_number) AS serial_number
    , COALESCE(rps.private_uuid, mdm.private_uuid) AS private_uuid
    , COALESCE(rps.is_active, mdm.is_active, FALSE) AS is_active
    , (rps._device_id_for_join IS NOT NULL) AS is_in_rps_services
    , (mdm._device_id_for_join IS NOT NULL) AS is_in_mdm
    , COALESCE(rps.vendor_id, mdm.vendor_id) AS vendor_id
    , rps.entity_id
    , rps.vendor_code
    , COALESCE(rps.model, mdm.model) AS model
    , IF(COALESCE(rps.manufacturer, mdm.manufacturer) = ''
        , NULL
        , COALESCE(rps.manufacturer, mdm.manufacturer)
      ) AS manufacturer
    , COALESCE(rps.os, mdm.os) AS os
    , COALESCE(rps.os_version, mdm.os_version) AS os_version
    , COALESCE(rps.total_memory, mdm.total_memory) AS total_memory
    , COALESCE(rps.total_storage, mdm.total_storage) AS total_storage
    , mdm.iccid
    , rps.display_resolution
    , rps.client_name
    , rps.client_version
    , rps.wrapper_type
    , rps.wrapper_version
    , rps.user_agent
  FROM rps_devices_clean rps
  LEFT JOIN mdm_devices mdm ON rps._join_key = mdm._device_id_for_join
    AND rps.country_code = mdm.country_code
), rps_devices_with_mdm_and_versions AS (
  SELECT *
    -- To mitigate datasource fault on the accuracy of data, we are using device_id as an identifier to NGT Devices.
    , (UPPER(SUBSTR(device_id, 1, 2)) = 'V2') AS is_ngt_device
    , ROW_NUMBER() OVER (PARTITION BY region, device_id ORDER BY GREATEST(assigned_at, COALESCE(updated_at, assigned_at)) DESC) AS _row_number_for_version
  FROM rps_devices_with_mdm
)
SELECT region
  , service
  , mdm_service
  , created_date
  , assigned_at
  , updated_at
  , country_code
  , device_id
  , serial_number
  , private_uuid
  , is_active
  , is_in_rps_services
  , is_in_mdm
  , is_ngt_device
  , (_row_number_for_version = 1) AS is_the_latest_version
  , vendor_id
  , entity_id
  , vendor_code
  , STRUCT(
      model
      , manufacturer
      , display_resolution
      , user_agent
      , total_memory
      , total_storage
      , iccid
    ) AS hardware
  , STRUCT(
      os AS name
      , os_version AS version
    ) AS os
  , STRUCT(
      client_name AS name
      , client_version AS version
      , IF(SUBSTR(wrapper_type, 1, 7) = 'WINDOWS', 'WINDOWS', wrapper_type) AS wrapper_type
      , wrapper_version
    ) AS client
FROM rps_devices_with_mdm_and_versions
