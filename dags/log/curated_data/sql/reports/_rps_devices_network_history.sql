CREATE OR REPLACE TABLE `{{ params.project_id }}.rl._rps_devices_network_history`
PARTITION BY created_date
CLUSTER BY region, device_id AS
WITH device_events_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl.rps_device_events` de
), device_network AS (
  SELECT de.created_date
    , de.region
    , de.entity_id
    , de.device_id
    , TIMESTAMP_TRUNC(de.created_at, HOUR) AS updated_hour
    , STRING_AGG(DISTINCT ni.type, '/' ORDER BY ni.type  ) AS type
    , STRING_AGG(DISTINCT ni.carrier ORDER BY ni.carrier  ) AS carrier
  FROM device_events_dataset de
  CROSS JOIN UNNEST(network_info) ni
  WHERE type != 'OTHER'
  GROUP BY 1,2,3,4,5
), device_network_lag AS ( 
  SELECT created_date
    , region
    , entity_id
    , device_id
    , updated_hour
    , type
    , carrier
    , LAG(type) OVER device_events_windows AS prev_type
    , LAG(carrier) OVER device_events_windows AS prev_carrier   
  FROM device_network
  WINDOW device_events_windows AS (
    PARTITION BY region, device_id
    ORDER BY updated_hour
  )
), device_network_update AS (
  SELECT *
  FROM device_network_lag
  WHERE (type != prev_type
      OR prev_type IS NULL )
    AND (carrier != prev_carrier
      OR prev_carrier IS NULL)
) 
SELECT created_date
  , region
  , entity_id
  , device_id
  , updated_hour
  , type
  , carrier
  , COALESCE(
      LEAD(updated_hour) OVER (PARTITION BY region, device_id ORDER BY updated_hour),
      CURRENT_TIMESTAMP
    ) AS next_updated_hour
FROM device_network_update
