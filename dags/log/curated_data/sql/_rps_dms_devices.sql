CREATE TEMP FUNCTION extract_model(_model STRING, _index INT64) AS (
  UPPER(
    IF(ARRAY_LENGTH(SPLIT(_model, '; ')) > 1
      , REPLACE(SPLIT(SPLIT(_model, '; ')[OFFSET(_index)], ': ')[OFFSET(1)], '"', '')
      , _model
    )
  )
);

CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._rps_dms_devices`
PARTITION BY created_date
CLUSTER BY region, device_id AS
WITH dms_devices_dataset AS (
  SELECT IF(region = 'asia', 'ap', region) AS region
    , 'DMS' AS service
    , COALESCE(SAFE_CAST(assignedAt AS DATE), SAFE_CAST(restaurantAssignment.assignedAt AS DATE)) AS created_date
    , COALESCE(assignedAt, restaurantAssignment.assignedAt) AS assigned_at
    , COALESCE(updatedAt, assignedAt, restaurantAssignment.assignedAt) AS updated_at
    , LOWER(countryCode) AS country_code
    , deviceUuid AS device_id
    , UPPER(deviceUuid) AS _device_id_for_join
    , privateUuid AS private_uuid
    , deviceUuid AS serial_number
    , COALESCE(SAFE_CAST(restaurantId AS INT64), restaurantAssignment.restaurantId) AS vendor_id
    , COALESCE(active = 1, FALSE) AS is_active
    , IF(metadata.mdm IN ('SOTI', 'MOBILOCK'), metadata.mdm, NULL) AS mdm_service
    , metadata.memorySize AS total_memory
    , metadata.diskSize AS total_storage
    , metadata.display AS display_resolution
    , IF(metadata.rpsClientAppType = 'WEBKICK', extract_model(metadata.model, 1), metadata.model) AS model
    , IF(metadata.rpsClientAppType = 'WEBKICK', extract_model(metadata.model, 0), NULL) AS manufacturer
    , metadata.operatingSystemName AS os
    , metadata.operatingSystemVersion AS os_version
    , IF(metadata.rpsClientAppType = 'WEBKICK', 'GOWIN', metadata.rpsClientAppType) AS client_name
    , metadata.rpsClientAppVersion AS client_version
    , IF(metadata.rpsClientAppType = 'WEBKICK' AND metadata.wrapperType IS NULL, 'WEB', metadata.wrapperType) AS wrapper_type
    , metadata.wrapperVersion AS wrapper_version
    , metadata.userAgent AS user_agent
    , version AS _data_version
  FROM `{{ params.project_id }}.dl.dms_device`
  WHERE deviceUuid <> 'lambda'
    AND UPPER(deviceUuid) NOT IN ('UNKNOWN', '0123456789ABCDEF')
    AND version IS NOT NULL
    -- we remove devices that are not assigned to any restaurant as it is not the focus of our business
    AND COALESCE(assignedAt, restaurantAssignment.assignedAt) IS NOT NULL
), dms_devices AS (
  SELECT * EXCEPT (model, manufacturer)
    -- temp solution before RPS Client teams have clean values in DMS
    , IF(model IN ('', 'UNKNOWN', 'O.E.M', 'TO BE FILLED BY O.E.M.', 'SYSTEM MANUFACTURER', 'NONE', 'DEFAULT STRING', 'SYSTEM PRODUCT NAME'),
        NULL,
        model
      ) AS model
    , IF(manufacturer IN ('', 'UNKNOWN', 'O.E.M', 'TO BE FILLED BY O.E.M.', 'SYSTEM MANUFACTURER', 'NONE', 'DEFAULT STRING', 'SYSTEM'),
        NULL,
        manufacturer
      ) AS manufacturer
  FROM (
    SELECT *
      -- This is necessary as there are different data version per updated records
      , ROW_NUMBER() OVER(PARTITION BY region, device_id, vendor_id, created_date, assigned_at, updated_at ORDER BY _data_version DESC) AS _row_number
    FROM dms_devices_dataset
    WHERE region IS NOT NULL
      AND vendor_id IS NOT NULL
      AND device_id IS NOT NULL
  )
  WHERE _row_number = 1
), dms_devices_latest_known_values AS (
  -- This is a workaround to fix missing updated record values by taking the last known values of each respective columns
  SELECT region
    , device_id
    , ARRAY_AGG(vendor_id IGNORE NULLS ORDER BY updated_at DESC)[SAFE_OFFSET(0)] AS vendor_id
    , ARRAY_AGG(mdm_service IGNORE NULLS ORDER BY updated_at DESC)[SAFE_OFFSET(0)] AS mdm_service
    , ARRAY_AGG(total_memory IGNORE NULLS ORDER BY updated_at DESC)[SAFE_OFFSET(0)] AS total_memory
    , ARRAY_AGG(total_storage IGNORE NULLS ORDER BY updated_at DESC)[SAFE_OFFSET(0)] AS total_storage
    , ARRAY_AGG(display_resolution IGNORE NULLS ORDER BY updated_at DESC)[SAFE_OFFSET(0)] AS display_resolution
    , ARRAY_AGG(os IGNORE NULLS ORDER BY updated_at DESC)[SAFE_OFFSET(0)] AS os
    , ARRAY_AGG(os_version IGNORE NULLS ORDER BY updated_at DESC)[SAFE_OFFSET(0)] AS os_version
    , ARRAY_AGG(client_name IGNORE NULLS ORDER BY updated_at DESC)[SAFE_OFFSET(0)] AS client_name
    , ARRAY_AGG(client_version IGNORE NULLS ORDER BY updated_at DESC)[SAFE_OFFSET(0)] AS client_version
    , ARRAY_AGG(wrapper_type IGNORE NULLS ORDER BY updated_at DESC)[SAFE_OFFSET(0)] AS wrapper_type
    , ARRAY_AGG(wrapper_version IGNORE NULLS ORDER BY updated_at DESC)[SAFE_OFFSET(0)] AS wrapper_version
    , ARRAY_AGG(user_agent IGNORE NULLS ORDER BY updated_at DESC)[SAFE_OFFSET(0)] AS user_agent
    , ARRAY_AGG(model IGNORE NULLS ORDER BY updated_at DESC)[SAFE_OFFSET(0)] AS model
    , ARRAY_AGG(manufacturer IGNORE NULLS ORDER BY updated_at DESC)[SAFE_OFFSET(0)] AS manufacturer
  FROM dms_devices
  WHERE region IS NOT NULL
    AND device_id IS NOT NULL
  GROUP BY 1,2
)
SELECT ori.region
  , ori.service
  , ori.created_date
  , ori.assigned_at
  , ori.updated_at
  , ori.country_code
  , ori.device_id
  , ori._device_id_for_join
  , ori.private_uuid
  , ori.serial_number
  , ori.vendor_id
  , ori.is_active
  , COALESCE(ori.mdm_service, fix.mdm_service) AS mdm_service
  , COALESCE(ori.total_memory, fix.total_memory) AS total_memory
  , COALESCE(ori.total_storage, fix.total_storage) AS total_storage
  , COALESCE(ori.display_resolution, fix.display_resolution) AS display_resolution
  , COALESCE(ori.os, fix.os) AS os
  , COALESCE(ori.os_version, fix.os_version) AS os_version
  , COALESCE(ori.client_name, fix.client_name) AS client_name
  , COALESCE(ori.client_version, fix.client_version) AS client_version
  , COALESCE(ori.wrapper_type, fix.wrapper_type) AS wrapper_type
  , COALESCE(ori.wrapper_version, fix.wrapper_version) AS wrapper_version
  , COALESCE(ori.user_agent, fix.user_agent) AS user_agent
  , COALESCE(ori.model, fix.model) AS model
  , COALESCE(ori.manufacturer, fix.manufacturer) AS manufacturer
FROM dms_devices ori
INNER JOIN dms_devices_latest_known_values fix USING(region, vendor_id, device_id)
