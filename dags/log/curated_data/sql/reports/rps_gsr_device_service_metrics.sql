CREATE TEMP FUNCTION parse_manufacturer(manufacturer STRING) AS (
  COALESCE( CASE regexp_replace(UPPER(SUBSTR(manufacturer, 1, 3)), '[^a-zA-Z0-9 -]', "")
              WHEN " " THEN "(N/A)"
              WHEN "" THEN"(N/A)"
              WHEN "UNK" THEN "(Unknown)"
              WHEN "UNB" THEN "(N/A)"
              WHEN "HP-" THEN "HEWLETT-PACKARD"
              WHEN "LG-" THEN "LG"
              ELSE manufacturer
            END
          , "(Unknown)")
);

CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.rps_gsr_device_service_metrics`
PARTITION BY created_date
CLUSTER BY entity_id, region, country_name, client_name AS
WITH date_variable AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 6 MONTH) AS date_st
    , DATE_SUB('{{ next_ds }}', INTERVAL 0 DAY) AS date_en
), entities AS (
  SELECT p.entity_id
    , e.country_iso
    , e.country_name
    , e.region
    , p.timezone
    , p.brand_name
  FROM `{{ params.project_id }}.cl.entities` e
  LEFT JOIN UNNEST(platforms) p 
), vendors AS (
  SELECT * EXCEPT(_row_number)
  FROM (
    SELECT v.entity_id
      , v.vendor_code
      , rps.region
      , rps.vendor_id
      , rps.country_iso
      , dal.city_name
      , ROW_NUMBER() OVER(PARTITION BY v.entity_id, v.vendor_code ORDER BY dal.city_id DESC) AS _row_number
    FROM `{{ params.project_id }}.cl.vendors_v2` v
    LEFT JOIN UNNEST(rps) rps
    LEFT JOIN UNNEST(delivery_areas_location) dal
  ) WHERE _row_number = 1
), vendor_attributes AS (
  SELECT entity_id
    , vendor_code
    , vendor_grade
  FROM `{{ params.project_id }}.rl.rps_vendor_attributes_latest`
), devices AS ( 
  SELECT *
    , COALESCE(
        LEAD(updated_at) OVER (PARTITION BY region, device_id ORDER BY updated_at), '{{ next_execution_date }}'
      ) AS next_updated_at
  FROM `{{ params.project_id }}.cl.rps_devices`
), device_events AS ( 
  SELECT *
  FROM `{{ params.project_id }}.rl._rps_devices_network_history`
), connectivity AS (
  SELECT DATE(s.starts_at) AS schedule_date
    , c.entity_id
    , c.vendor_code
    , c.region
    , c.vendor_id
    , AVG(COALESCE(s.daily_schedule_duration, 0)) AS daily_schedule_scnd
    , SUM(s.connectivity.unreachable_duration) AS daily_unreachable_second
    , SUM(s.availability.offline_duration) AS daily_offline_second
  FROM `{{ params.project_id }}.cl.rps_connectivity` c
  CROSS JOIN date_variable vars
  CROSS JOIN UNNEST(slots_local) s
  WHERE DATE(s.starts_at) >= vars.date_st
    AND DATE(s.starts_at) < vars.date_en
  GROUP BY 1,2,3,4,5
), orders AS (
  SELECT DATE(o.created_at, o.timezone) AS created_date
    , o.timezone
    , o.created_at
    , o.region
    , o.country_iso
    , o.entity.id AS entity_id
    , o.order_status
    , o.cancellation.owner AS cancellation_owner
    , o.cancellation.reason As cancellation_reason
    , o.order_id
    , o.vendor.code AS vendor_code
    , o.vendor.id AS vendor_id
    , o.device.id AS device_id
    , o.client.name AS client_name
    , o.client.version AS client_version
    , o.client.wrapper_type AS client_wrapper_type
    , o.client.wrapper_version AS client_wrapper_version
    , o.client.pos_integration_flow
  FROM `{{ params.project_id }}.cl.rps_orders` o
  CROSS JOIN date_variable vars
  WHERE o.created_date >= vars.date_st
    AND o.created_date < vars.date_en
), orders_devices AS (
  SELECT o.*
    , d.service
    , d.mdm_service
    , d.hardware.model AS hw_model
    , d.is_ngt_device AS is_ngt
    , TRIM(SPLIT(d.hardware.manufacturer,' ')[OFFSET(0)]) AS hw_manufacturer
    , d.os.name AS os_name
    , d.os.version AS os_version
    , IF( de.type LIKE '%MOBILE%'
        , CASE SUBSTR(d.hardware.iccid, 1, 7)
            WHEN '8910390' THEN 'NEW GLOBAL SIM' 
            WHEN '8988303' THEN 'OLD GLOBAL SIM'
            ELSE 'LOCAL SIM'
          END
        , de.type
      ) AS network_channel
  FROM orders o
  LEFT JOIN devices d ON o.device_id = d.device_id
    AND o.region = d.region
    AND (o.created_at >= d.updated_at AND o.created_at < d.next_updated_at)
  LEFT JOIN device_events de ON o.device_id = de.device_id
    AND o.region = de.region
    AND (o.created_at >= de.updated_hour AND o.created_at < de.next_updated_hour)
), vendors_clean AS (
  SELECT v.* EXCEPT(country_iso)
    , e.country_name
    , COALESCE(att.vendor_grade, 'NA') AS vendor_grade
  FROM vendors v
  INNER JOIN entities e ON e.entity_id = v.entity_id
  LEFT JOIN vendor_attributes att ON v.entity_id = att.entity_id
    AND v.vendor_code = att.vendor_code
), all_clean AS (
  SELECT od.created_date
    , od.region
    , od.country_iso
    , v.country_name
    , od.entity_id
    , od.vendor_code
    , od.device_id
    , od.vendor_id
    , CASE 
        WHEN od.client_name = 'POS'
          THEN CONCAT(od.client_name, ' ', od.pos_integration_flow)
        WHEN od.client_name = 'GOWIN'
          THEN CONCAT(od.client_name, ' ',
          CASE 
            WHEN SUBSTR(od.client_wrapper_type, 1, 7) = 'WINDOWS'
              THEN 'WINDOWS'
            WHEN od.client_wrapper_type = 'NONE'
              THEN ''
            ELSE COALESCE(od.client_wrapper_type, 'UNKNWON') 
          END)
        ELSE COALESCE(od.client_name, "(Unknown)")
      END AS client_name
    , COALESCE(od.client_version, "(Unknown)") AS app_version
    , COALESCE(v.city_name, "(Unknown)") AS city_name
    , v.vendor_grade
    , COALESCE(od.os_name , "(Unknown)") AS os_name
    , COALESCE(od.os_version, "(Unknown)") AS os_version
    , COALESCE(od.mdm_service, "NONE") AS mdm_service
    , parse_manufacturer(od.hw_manufacturer) AS hw_manufacturer
    , COALESCE(od.network_channel, "(Unknown)") AS network_channel
    , od.is_ngt
    , COALESCE(od.service, 'NONE') AS service
    , od.order_id
    , od.order_status
    , od.cancellation_reason
    , od.cancellation_owner
    , c.daily_schedule_scnd
    , c.daily_unreachable_second
    , c.daily_offline_second
  FROM orders_devices od
  INNER JOIN vendors_clean v USING(entity_id, vendor_code)
  LEFT JOIN connectivity c ON od.entity_id = c.entity_id
    AND od.vendor_code = c.vendor_code
    AND od.created_date = c.schedule_date
), aggregation AS (
  SELECT created_date
    , entity_id
    , city_name
    , vendor_grade
    , client_name
    , app_version
    , os_name
    , os_version
    , mdm_service
    , hw_manufacturer
    , network_channel
    , is_ngt
    , service
    -- orders metrics
    , COUNT(DISTINCT order_id) AS no_orders
    , COUNT(DISTINCT IF(order_status <> 'completed', order_id, NULL)) AS no_failed_orders
    , COUNT(DISTINCT IF(cancellation_reason = 'UNREACHABLE', order_id, NULL)) AS no_failed_unreachable_orders
    , COUNT(DISTINCT IF(cancellation_owner = 'VENDOR', order_id, NULL)) AS no_failed_vendors_orders
    -- vendor metrics
    , COUNT(DISTINCT CONCAT(entity_id, vendor_code)) AS no_daily_vendors
    , COUNT(DISTINCT IF(service = 'DMS', CONCAT(entity_id, vendor_code), NULL)) AS no_daily_dms_vendors
    , COUNT(DISTINCT IF(is_ngt, CONCAT(entity_id, vendor_code), NULL)) AS no_daily_ngt_vendors
    , COUNT(DISTINCT IF(NOT(is_ngt), CONCAT(entity_id, vendor_code), NULL)) AS no_daily_non_ngt_vendors
    , COUNT(DISTINCT IF(mdm_service = 'SOTI', CONCAT(entity_id, vendor_code), NULL)) AS no_daily_soti_vendors
    , COUNT(DISTINCT IF(network_channel = 'GLOBAL SIM', CONCAT(entity_id, vendor_code), NULL)) AS no_daily_global_sim_vendors
    -- device metrics
    , COUNT(DISTINCT CONCAT(region, device_id)) AS no_daily_devices
    , COUNT(DISTINCT IF(service = 'DMS', CONCAT(region, device_id), NULL)) AS no_daily_dms_devices
    , COUNT(DISTINCT IF(is_ngt, CONCAT(region, device_id), NULL)) AS no_daily_ngt_devices
    , COUNT(DISTINCT IF(NOT(is_ngt), CONCAT(region, device_id), NULL)) AS no_daily_non_ngt_devices
    , COUNT(DISTINCT IF(mdm_service = 'SOTI', CONCAT(region, device_id), NULL)) AS no_daily_soti_devices
    , COUNT(DISTINCT IF(network_channel = 'GLOBAL SIM', CONCAT(region, device_id), NULL)) AS no_daily_global_sim_devices
    -- connectivity metrics
    , SUM(daily_schedule_scnd/60) AS mnt_daily_schedule
    , SUM(daily_unreachable_second/60) mnt_daily_unreachable
    , SUM(daily_offline_second/60) mnt_daily_offline
  FROM all_clean
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
)
SELECT agg.*
  , dim.region
  , dim.country_name
  , dim.brand_name
FROM aggregation agg
INNER JOIN entities dim ON agg.entity_id = dim.entity_id
