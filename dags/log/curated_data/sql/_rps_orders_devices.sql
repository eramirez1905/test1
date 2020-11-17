CREATE TEMP FUNCTION get_region_short_name(_region STRING) AS (
  IF(_region = 'asia', 'ap', _region)
);

CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._rps_orders_devices`
CLUSTER BY region, device_id, order_id AS
WITH rps_devices_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl.rps_devices`
), rps_vendor_client_events_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl.rps_vendor_client_events`
), dms_push_notification_sent_message_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.dl.dms_push_notification_sent_message`
), devices AS (
  SELECT updated_at
    , region
    , device_id
    , serial_number
    , mdm_service
    , hardware.manufacturer
    , hardware.model
    , is_ngt_device
    , os.name AS os_name
    , os.version AS os_version
    , client.name AS client_name
    -- Temp solution before RPSBI-780 is done
    , IF(STARTS_WITH(client.version, 'release/'), NULL, client.version) AS client_version
    , client.wrapper_type AS client_wrapper_type
    , client.wrapper_version AS client_wrapper_version
    , COALESCE(LEAD(updated_at) OVER (PARTITION BY region, device_id ORDER BY updated_at)
        , '{{ next_execution_date }}'
      ) AS next_updated_at
  FROM rps_devices_dataset
), orders_devices_from_dms AS (
  SELECT * EXCEPT(_row_number)
  FROM(
    SELECT created_at
      , get_region_short_name(region) AS region
      , TRIM(orderId) AS order_id
      , TRIM(deviceUuid) AS device_id
      , ROW_NUMBER() OVER (PARTITION BY TRIM(orderId) ORDER BY created_at DESC) AS _row_number
    FROM dms_push_notification_sent_message_dataset
  )
  WHERE _row_number = 1
), orders_devices_from_vendor_client_events AS (
  SELECT * EXCEPT(_row_number)
  FROM (
    SELECT created_at
      , region
      , orders.id AS order_id
      , TRIM(device.id) AS device_id
      , ROW_NUMBER() OVER (PARTITION BY orders.id ORDER BY created_at DESC) AS _row_number
    FROM rps_vendor_client_events_dataset
    WHERE orders.id IS NOT NULL
  )
  WHERE _row_number = 1
), orders_devices AS (
  SELECT * EXCEPT(_row_number)
  FROM(
    SELECT *
      , ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY created_at DESC) AS _row_number
    FROM (
      SELECT created_at
        , region
        , order_id
        , device_id
      FROM orders_devices_from_dms
      UNION ALL
      SELECT created_at
        , region
        , order_id
        , device_id
      FROM orders_devices_from_vendor_client_events

    )
  )
  WHERE _row_number = 1
)
SELECT o.region
  , o.order_id
  , o.device_id
  , d.serial_number
  , d.mdm_service
  , d.manufacturer
  , d.model
  -- Temp solution until data quality in `rps_devices` is fixed - there are missing devices from DMS for unknown reason
  , (STARTS_WITH(UPPER(o.device_id), 'V2')) AS is_ngt_device
  , d.os_name
  , d.os_version
  , d.client_name
  , d.client_version
  , d.client_wrapper_type
  , d.client_wrapper_version
FROM orders_devices o
LEFT JOIN devices d ON o.region = d.region
  AND o.device_id = d.device_id
  AND o.created_at BETWEEN d.updated_at AND d.next_updated_at
