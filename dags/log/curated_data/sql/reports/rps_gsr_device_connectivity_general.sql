CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.rps_gsr_device_connectivity_general`
PARTITION BY date
CLUSTER BY entity_id, region, delivery_platform, vendor_uid AS
WITH vars AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 3 MONTH) AS partition_st
    , DATE_SUB('{{ next_ds }}', INTERVAL 0 DAY) AS partition_en
), entities AS (
  SELECT * EXCEPT(platforms)
  FROM `{{ params.project_id }}.cl.entities` e
  LEFT JOIN UNNEST(platforms) pl
), conn_raw AS (
  SELECT DATE(s.starts_at) AS schedule_date
    , c.entity_id
    , UPPER(TRIM(CONCAT(c.entity_id, c.vendor_code))) AS vendor_uid
    , AVG(COALESCE(s.daily_schedule_duration, 0)) AS daily_schedule_scnd
    , IF( SUM(s.connectivity.unreachable_duration) > AVG(COALESCE(s.daily_schedule_duration, 0))
        , AVG(COALESCE(s.daily_schedule_duration, 0))
        , SUM(s.connectivity.unreachable_duration)
      ) AS daily_unreachable_second
    , IF( SUM(s.availability.offline_duration) > AVG(COALESCE(s.daily_schedule_duration, 0))
        , AVG(COALESCE(s.daily_schedule_duration, 0))
        , SUM(s.availability.offline_duration)
      ) AS daily_offline_second
    , COUNTIF(s.connectivity.is_unreachable) AS daily_unreachable_cnt
    , COUNTIF(s.availability.is_offline) AS daily_offline_cnt
    , COUNTIF(s.connectivity.is_unreachable AND s.connectivity.unreachable_duration < 1800) AS daily_short_unreachable_cnt
    , COUNTIF(s.availability.is_offline AND s.availability.offline_duration < 1800) AS daily_short_offline_cnt
    , COUNTIF(s.connectivity.is_unreachable
        AND s.connectivity.unreachable_duration >= 1800
        AND s.connectivity.unreachable_duration <> s.daily_schedule_duration
      ) AS daily_long_unreachable_cnt
    , COUNTIF(s.availability.is_offline
        AND s.availability.offline_duration >= 1800
        AND s.availability.offline_duration <> s.daily_schedule_duration
      ) AS daily_long_offline_cnt
    , COUNTIF(s.connectivity.is_unreachable AND s.connectivity.unreachable_duration = s.daily_schedule_duration) AS daily_allday_unreachable_cnt
    , COUNTIF(s.availability.is_offline AND s.availability.offline_duration = s.daily_schedule_duration) AS daily_allday_offline_cnt
  FROM `{{ params.project_id }}.cl.rps_connectivity` c
  CROSS JOIN vars
  LEFT JOIN UNNEST(slots_local) s
  WHERE DATE(s.starts_at) >= vars.partition_st
    AND DATE(s.starts_at) < vars.partition_en
  GROUP BY 1,2,3
), vendors_raw AS (
  SELECT UPPER(TRIM(CONCAT(v.entity_id, v.vendor_code))) AS vendor_uid
    , v.is_monitor_enabled
    , rps.region
    , c.country_name
    , e.brand_name AS delivery_platform
    , dal.city_name
    , COALESCE(att.vendor_grade, 'NA') AS vendor_grade
  FROM `{{ params.project_id }}.cl.vendors_v2` v
  LEFT JOIN UNNEST(rps) rps
  LEFT JOIN UNNEST(delivery_areas_location) dal
  INNER JOIN entities e ON e.entity_id = v.entity_id
  LEFT JOIN `{{ params.project_id }}.rl.rps_vendor_attributes_latest` att ON v.entity_id = att.entity_id
    AND v.vendor_code = att.vendor_code
  LEFT JOIN `{{ params.project_id }}.cl.countries` c ON rps.country_iso = c.country_iso
  WHERE rps.is_latest
  GROUP BY 1,2,3,4,5,6,7
), network_info AS (
  SELECT * 
  FROM `{{ params.project_id }}.rl._rps_devices_network_history`
), devices_raw AS (
  SELECT *
    , UPPER(TRIM(CONCAT(entity_id, vendor_code))) AS vendor_uid 
    , COALESCE(
        LEAD(updated_at) OVER (PARTITION BY region, device_id ORDER BY updated_at), '{{ next_execution_date }}'
      ) AS next_updated_at
  FROM `{{ params.project_id }}.cl.rps_devices`
), orders_raw AS (
  SELECT DATE(o.created_at, o.timezone) AS created_date
    , o.created_at
    , UPPER(TRIM(CONCAT(o.entity.id, o.vendor.code))) AS vendor_uid
    , o.region
    , o.device.id AS device_id
    , IF( SUBSTR(o.client.wrapper_type, 1, 7) = 'WINDOWS', 'WINDOWS', o.client.wrapper_type) AS client_wrapper_type
    , IF( o.client.name <> 'POS', o.client.name, CONCAT(o.client.name, ' ', o.client.pos_integration_flow)) AS client_name
    , o.client.version AS application_version
    , SPLIT(o.device.manufacturer,' ')[OFFSET(0)] AS manufacturer
    , o.device.mdm_service
    , o.order_status AS status
    , o.order_id
    , (o.order_status = 'completed' AND o.received_by_vendor_at IS NULL) AS is_saved_order
    , (o.order_status = 'cancelled' AND cancellation.reason ='UNREACHABLE') AS is_unreachable_order
  FROM `{{ params.project_id }}.cl.rps_orders` o
  CROSS JOIN vars
  WHERE o.created_date >= vars.partition_st
    AND o.created_date < vars.partition_en
), orders_devices AS (
  SELECT odr.created_date
    , odr.vendor_uid
    , TRIM(CONCAT(COALESCE(odr.client_wrapper_type, ''), ' ', odr.client_name)) AS client_name
    , odr.application_version
    , odr.manufacturer
    , odr.mdm_service
    , IF( ni.type LIKE '%MOBILE%'
        , CASE SUBSTR(d.hardware.iccid, 1, 7)
            WHEN '8910390' THEN 'NEW GLOBAL SIM' 
            WHEN '8988303' THEN 'OLD GLOBAL SIM'
            ELSE 'LOCAL SIM'
          END
        , ni.type
      ) AS network_channel
    , COUNT(DISTINCT odr.order_id) AS no_order
    , COUNT(DISTINCT IF(is_saved_order, odr.order_id, NULL)) AS no_saved_order
    , COUNT(DISTINCT IF(is_unreachable_order, odr.order_id, NULL)) AS no_unreachable_order
  FROM orders_raw odr
  LEFT JOIN devices_raw d ON odr.device_id = d.device_id
    AND odr.region = d.region
    AND (odr.created_at >= d.updated_at AND odr.created_at < d.next_updated_at)
  LEFT JOIN network_info ni ON odr.device_id = ni.device_id
    AND odr.region = ni.region
    AND (odr.created_at >= ni.updated_hour AND odr.created_at < ni.next_updated_hour)
  GROUP BY 1,2,3,4,5,6,7
), orders_devices_cleaned AS (
  SELECT DISTINCT created_date
    , vendor_uid
    , client_name
    , application_version
    , manufacturer
    , mdm_service
    , network_channel
    , no_order
    , SUM(no_order) 
        OVER (
          PARTITION BY vendor_uid
          ORDER BY created_date ROWS BETWEEN 28 PRECEDING AND CURRENT ROW
        ) AS no_last28days_order    
    , no_saved_order
    , no_unreachable_order
  FROM orders_devices
), city_maturity AS (
  SELECT vr.city_name
    , DATE_DIFF(vars.partition_en, MIN(odr.created_date), DAY) AS city_age_days
  FROM vendors_raw vr
  CROSS JOIN vars
  LEFT JOIN orders_raw odr ON vr.vendor_uid = odr.vendor_uid
  WHERE odr.status = 'completed'
  GROUP BY 1, vars.partition_en
)
SELECT * EXCEPT(_rn)
FROM (
  SELECT DISTINCT cr.vendor_uid
    , cr.schedule_date AS date
    , vr.is_monitor_enabled
    , cr.entity_id
    , COALESCE(vr.region, '(Unknown)') AS region
    , COALESCE(vr.country_name, '(Unknown)') AS country
    , COALESCE(vr.city_name, '(Unknown)') AS city
    , COALESCE(vr.delivery_platform, '(Unknown)') AS delivery_platform
    , COALESCE(vr.vendor_grade, 'NA') AS vendor_segment
    , COALESCE(od.client_name, '(Unknown)') AS client_name
    , COALESCE(od.application_version, '(Unknown)') AS application_version
    , COALESCE(od.manufacturer, '(Unknown)') AS manufacturer
    , COALESCE(od.mdm_service, '(Unknown)') AS mdm_service
    , COALESCE(od.network_channel, '(Unknown)') AS network_channel
    , COALESCE(
        CASE 
          WHEN cm.city_age_days <= 28 
            THEN 'NEWLY-LAUNCHED'
          WHEN cm.city_age_days > 28 AND cm.city_age_days <= 90 
            THEN 'RAMPING-UP'
          ELSE 'MATURE'
        END
      , '(Unknown)'
      ) AS city_maturity
    , cr.daily_schedule_scnd AS schedule_duration_seconds
    , cr.daily_unreachable_second AS unreachable_duration_seconds
    , cr.daily_offline_second AS offline_duration_seconds
    , cr.daily_unreachable_cnt AS no_unreachable_events
    , cr.daily_offline_cnt AS no_offline_events
    , COALESCE(od.no_order, 0) AS no_orders
    , COALESCE(od.no_saved_order, 0) AS no_saved_orders
    , COALESCE(od.no_unreachable_order, 0) AS no_unreachable_orders
    , cr.daily_short_unreachable_cnt AS no_short_unreachable_cnt
    , cr.daily_short_offline_cnt AS no_short_offline_cnt
    , cr.daily_long_unreachable_cnt AS no_long_unreachable_cnt
    , cr.daily_long_offline_cnt AS no_long_offline_cnt
    , cr.daily_allday_unreachable_cnt AS no_allday_unreachable_cnt
    , cr.daily_allday_offline_cnt AS no_allday_offline_cnt
    , COALESCE(od.no_last28days_order, 0) AS no_last28days_order
    , ROW_NUMBER() OVER( 
          PARTITION BY cr.vendor_uid, cr.schedule_date
          ORDER BY vr.region DESC, vr.country_name DESC, vr.city_name DESC, vr.delivery_platform DESC, vr.vendor_grade DESC
            , od.client_name DESC, od.application_version DESC, od.manufacturer DESC, od.mdm_service DESC
            , od.network_channel DESC, cm.city_age_days DESC
      ) AS _rn
  FROM conn_raw cr
  LEFT JOIN orders_devices_cleaned od ON cr.schedule_date = od.created_date
    AND cr.vendor_uid = od.vendor_uid
  LEFT JOIN vendors_raw vr ON cr.vendor_uid = vr.vendor_uid
  LEFT JOIN city_maturity cm ON vr.city_name = cm.city_name
) 
WHERE _rn = 1
