CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.rps_gsr_client_dashboard_overview`
PARTITION BY created_date
CLUSTER BY entity_id, region, brand_name, country_name AS
WITH entities AS (
  SELECT en.region_short_name AS region
    , en.country_iso
    , LOWER(en.country_iso) AS country_code
    , en.country_name
    , p.entity_id
    , p.brand_name
    , p.timezone
  FROM `{{ params.project_id }}.cl.entities` en
  CROSS JOIN UNNEST(platforms) p
), device_mdm AS (
  SELECT * EXCEPT(_row_number)
  FROM (
    SELECT created_date
      , region
      , mdm_service
      , device_id
      , vendor_id
      , ROW_NUMBER() OVER (PARTITION BY created_date, region, vendor_id ORDER BY assigned_at DESC, updated_at DESC) AS _row_number
    FROM `{{ params.project_id }}.cl.rps_devices`
  )
  WHERE _row_number = 1
), rps_vendor_client_events_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl.rps_vendor_client_events`
  WHERE created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 3 MONTH)
    AND region IS NOT NULL
    AND vendor_id IS NOT NULL
), rps_vendor_client_events AS (
  SELECT DISTINCT DATE(ev.created_at, ev.timezone) AS created_date
    , ev.region
    , ev.vendor_id
    , ev.device.id AS device_id
    , ev.entity_id
    , CASE
        WHEN ev.client.name = 'GODROID'
          THEN 'GODROID'
        WHEN ev.client.name = 'GOWIN' AND ev.client.wrapper_type = 'WINDOWS'
          THEN 'GOWIN'
        WHEN ev.client.name = 'GOWIN' AND ev.client.wrapper_type = 'WEB'
          THEN 'GOWEB'
      END AS client_name
    , ev.client.version AS client_version
    , COALESCE(ev.client.wrapper_type, '(N/A)') AS wrapper_type
    , COALESCE(ev.client.wrapper_version, '(N/A)') AS wrapper_version
    , COALESCE(ev.device.os_name, '(UNKNOWN)') AS os_name
    , COALESCE(ev.device.os_version, '(UNKNOWN)') AS os_version
    , COALESCE(ev.device.model, '(UNKNOWN)') AS device_model
    , COALESCE(ev.device.brand, '(UNKNOWN)') AS device_brand
    , CONCAT(ev.region , '-', ev.vendor_id ) AS uniqule_vendor_id
  FROM rps_vendor_client_events_dataset ev
), users_base AS (
  SELECT DISTINCT created_date
    , region
    , vendor_id
  FROM rps_vendor_client_events
), users_with_mixed_profile AS (
  SELECT created_date
    , region
    , vendor_id
    , COUNT(DISTINCT client_name) AS mixed_user_clients_count
  FROM (
    SELECT DISTINCT created_date
      , region
      , vendor_id
      , client_name
    FROM rps_vendor_client_events
  )
  GROUP BY 1,2,3
  HAVING mixed_user_clients_count >1
), rps_vendor_client_events_clean AS (
  SELECT *
  FROM (
    SELECT *
      , ROW_NUMBER() OVER (PARTITION BY created_date, region, vendor_id ORDER BY created_date DESC) AS _row_number
    FROM rps_vendor_client_events
  )
  WHERE _row_number = 1
), rps_vendor_client_events_with_mixed AS (
  SELECT ev.*
    , IF(mx.vendor_id IS NOT NULL, 'MIXED', client_name) AS client_profile
    , mx.mixed_user_clients_count
  FROM rps_vendor_client_events_clean ev
  LEFT JOIN users_with_mixed_profile mx ON ev.vendor_id = mx.vendor_id
    AND ev.region = mx.region
    AND ev.created_date = mx.created_date
), rps_vendor_client_events_for_agg AS (
  SELECT b.created_date
    , ev.entity_id
    , ev.client_version
    , ev.wrapper_type
    , ev.wrapper_version
    , ev.os_name
    , ev.os_version
    , COALESCE(mdm.mdm_service, '(UNKNOWN)') AS mdm_service
    , ev.device_model
    , ev.device_brand
    , COUNT(DISTINCT ev.uniqule_vendor_id) AS users_ct
    , COUNT(DISTINCT IF(ev.client_profile = 'GODROID', ev.uniqule_vendor_id, NULL)) AS users_godroid_ct
    , COUNT(DISTINCT IF(ev.client_profile = 'GOWIN', ev.uniqule_vendor_id, NULL)) AS users_gowin_ct
    , COUNT(DISTINCT IF(ev.client_profile = 'GOWEB', ev.uniqule_vendor_id, NULL)) AS users_goweb_ct
    , COUNT(DISTINCT IF(ev.client_profile = 'MIXED', ev.uniqule_vendor_id, NULL)) AS users_mixed_ct
    , COUNT(*) AS event_ct
  FROM users_base b
  INNER JOIN rps_vendor_client_events_with_mixed ev ON b.vendor_id = ev.vendor_id
    AND b.region = ev.region
    AND b.created_date = ev.created_date
  LEFT JOIN device_mdm mdm ON ev.device_id = mdm.device_id
    AND ev.region = mdm.region
    AND ev.created_date >= mdm.created_date
  GROUP BY 1,2,3,4,5,6,7,8,9,10
)
SELECT ev.*
  , EXTRACT(YEAR FROM ev.created_date) AS created_year
  , FORMAT_DATE('%Y-%m', ev.created_date) AS created_month
  , FORMAT_DATE('%Y-%V', ev.created_date) AS created_week
  , en.region
  , en.brand_name
  , en.country_name
FROM rps_vendor_client_events_for_agg ev
INNER JOIN entities en ON ev.entity_id = en.entity_id
