CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.rps_datastudio_device_status_trend`
PARTITION BY created_date
CLUSTER BY region, country_name, vendor_code, serial_number AS
WITH entities AS (
  SELECT e.region_short_name
    , e.country_name
    , p.entity_id
    , e.country_iso
    , p.timezone
  FROM `{{ params.project_id }}.cl.entities` e
  LEFT JOIN UNNEST(platforms) p 
), rps_devices AS ( 
  SELECT *
  FROM `{{ params.project_id }}.cl.rps_devices`
  WHERE is_the_latest_version
), rps_device_events AS ( 
  SELECT *
  FROM `{{ params.project_id }}.cl.rps_device_events`
  WHERE created_date BETWEEN DATE_SUB('{{ next_ds }}', INTERVAL 7 DAY) AND '{{ next_ds }}'
), device_events AS (
  SELECT de.created_date
    , de.created_at
    , de.region
    , de.entity_id
    , de.vendor_code
    , de.device_id
    , de.available_memory
    , de.battery_level
    , de.is_charging
    , de.device_orientation
    , de.locale
    , de.volume_level 
    , ni.type
    , ni.carrier
    , ni.signal_strength
    , pi.load_percentage AS total_load_percentage
  FROM rps_device_events de
  LEFT JOIN UNNEST(network_info) ni
  LEFT JOIN UNNEST(processor_info) pi
), devices AS (
  SELECT region
    , entity_id
    , vendor_code
    , serial_number
    , device_id
    , hardware.model
    , hardware.manufacturer
    , hardware.total_storage 
    , mdm_service
    , client.name AS client_name
    , client.wrapper_type AS client_wrapper_type
    , client.version AS application_version
    , hardware.iccid
    , hardware.total_memory
    , hardware.display_resolution
    , hardware.user_agent
    , os.name AS os_name
    , os.version AS os_version
  FROM rps_devices
)
SELECT de.created_date
  , IF(de.created_date = '{{ next_ds }}', EXTRACT(HOUR FROM DATETIME(de.created_at, e.timezone)), NULL) AS hour_of_current_date
  , IF(de.created_date = '{{ next_ds }}', 'Hourly', 'Daily') AS data_type
  , de.* EXCEPT(created_date, created_at)
  , e.country_name
  , d.* EXCEPT(entity_id, vendor_code, region, device_id)
  , IF(de.type = 'MOBILE'
      , IF(SUBSTR(d.iccid, 1, 7) IN ('8910390', '8988303'), 'GLOBAL SIM', 'LOCAL SIM')
      , de.type
    ) AS network_channel
FROM device_events de  
INNER JOIN devices d ON de.region = d.region
  AND de.device_id = d.device_id
INNER JOIN entities e ON de.entity_id = e.entity_id
