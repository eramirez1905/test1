WITH rps_devices_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl.rps_devices`
), dms_device_status_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.dl.dms_device_status`
{%- if not params.backfill %}
  WHERE created_date BETWEEN DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY) AND '{{ next_ds }}'
{%- endif %}
), rps_devices AS (
  SELECT updated_at
    , region
    , country_code
    , vendor_id
    , entity_id
    , vendor_code
    , device_id 
    , COALESCE(LEAD(updated_at) OVER (PARTITION BY region, device_id ORDER BY updated_at)
        , '{{ next_execution_date }}'
      ) AS next_updated_at
  FROM rps_devices_dataset
), rps_dms_device_status As (
  SELECT d.region
    , d.country_code
    , d.vendor_id
    , d.entity_id
    , d.vendor_code
    , d.device_id 
    , ds.created_date
    , ds.created_at
    , ds.statusUuid AS status_id
    , ds.availableMemory AS available_memory
    , ds.batteryLevel AS battery_level
    , (ds.charging = 1) AS is_charging
    , ds.deviceOrientation AS device_orientation
    , LOWER(ds.locale) AS locale
    , ds.volumeLevel AS volume_level
    , ds.networkInfo AS network_info
    , ds.processorInfo AS processor_info
  FROM dms_device_status_dataset ds 
  INNER JOIN rps_devices d ON ds.deviceUuid = d.device_id
    AND IF(ds.region = 'asia', 'ap', ds.region) = d.region
    AND (ds.created_at >= d.updated_at AND ds.created_at < d.next_updated_at)
), rps_dms_device_status_processor_info AS (
  SELECT region
    , device_id
    , status_id
    , ARRAY_AGG(
        STRUCT(
          pi.label
          , pi.loadPercentage AS load_percentage
        )
      ) AS processor_info
  FROM rps_dms_device_status
  CROSS JOIN UNNEST (processor_info) pi
  GROUP BY 1,2,3
), rps_dms_device_status_network_info AS (
  SELECT region
    , device_id
    , status_id
    , ARRAY_AGG(
        STRUCT(
          ni.type
          , ni.protocol AS cellular_protocol
          , ni.carrier
          , ni.strength AS signal_strength
        )
      ) AS network_info
  FROM rps_dms_device_status
  CROSS JOIN UNNEST (network_info) ni
  GROUP BY 1,2,3
), rps_dms_device_status_network_info_ipAddresses AS (
  SELECT region
    , device_id
    , status_id
    , ARRAY_AGG(
        STRUCT(
          ip.format
          , ip.address
        )
      ) AS mac_address
  FROM rps_dms_device_status
  CROSS JOIN UNNEST (network_info) ni
  CROSS JOIN UNNEST (ni.ipAddresses) ip
  GROUP BY 1,2,3
)
SELECT dds.region
  , dds.country_code
  , dds.vendor_id
  , dds.entity_id
  , dds.vendor_code
  , dds.created_date
  , dds.created_at
  , dds.device_id
  , dds.status_id
  , dds.available_memory
  , dds.battery_level
  , dds.is_charging
  , dds.device_orientation
  , dds.locale
  , dds.volume_level
  , ddsni.network_info
  , ddsniip.mac_address
  , ddspi.processor_info
FROM rps_dms_device_status dds
LEFT JOIN rps_dms_device_status_processor_info ddspi ON dds.status_id = ddspi.status_id
  AND dds.device_id = ddspi.device_id
  AND dds.region = ddspi.region
LEFT JOIN rps_dms_device_status_network_info ddsni ON dds.status_id = ddsni.status_id
  AND dds.device_id = ddsni.device_id
  AND dds.region = ddsni.region
LEFT JOIN rps_dms_device_status_network_info_ipAddresses ddsniip ON dds.status_id = ddsniip.status_id
  AND dds.device_id = ddsniip.device_id
  AND dds.region = ddsniip.region
