CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.rps_backoffice_sessions`
PARTITION BY created_date AS
WITH rbo_session_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.dl.rps_backoffice_sessions`
), rbo_sessions AS (
  SELECT created_date
    , TIMESTAMP_SECONDS(visitStartTime) AS created_at
    , clientId AS client_id
    , CONCAT(fullVisitorId, '-', CAST(visitId AS STRING)) AS session_id
    , visitNumber AS visit_number
    , CAST(totals.visits IS NOT NULL AS BOOL) AS has_intraction
    , CAST(totals.newVisits IS NOT NULL AS BOOL) AS is_first_visit
    , CAST(totals.bounces IS NOT NULL AS BOOL) AS is_bounce
    , totals.hits AS total_hits
    , totals.pageviews
    , totals.timeOnSite AS time_on_site
    , device.browser
    , device.browserSize AS browser_size
    , device.mobileDeviceInfo AS mobile_device_info
    , device.mobileDeviceMarketingName AS mobile_device_marketing_name
    , device.mobileDeviceModel AS mobile_device_model
    , device.mobileInputSelector AS mobile_input_selector
    , device.operatingSystem AS os
    , device.operatingSystemVersion AS os_version
    , device.mobileDeviceBranding AS mobile_device_branding
    , device.language
    , device.screenResolution AS screen_resolution
    , device.screenColors AS screen_colors
    , (SELECT SPLIT(page.hostname, '.')[OFFSET(0)] FROM UNNEST(hits) ORDER BY hitNumber LIMIT 1) AS region
    , hits
  FROM rbo_session_dataset
), rbo_events AS (
  -- The table needs to be broken down to hits/event level first, then we could agg it back to session level
  SELECT s.created_date
    , s.created_at
    , s.client_id
    , s.session_id
    , s.visit_number
    , s.has_intraction
    , s.is_first_visit
    , s.is_bounce
    , s.total_hits
    , s.pageviews
    , s.time_on_site
    , s.browser
    , s.browser_size
    , s.mobile_device_info
    , s.mobile_device_marketing_name
    , s.mobile_device_model
    , s.mobile_input_selector
    , s.os
    , s.os_version
    , s.mobile_device_branding
    , s.language
    , s.screen_resolution
    , s.screen_colors
    , CONCAT(s.session_id, CAST(h.hitNumber AS STRING), h.type) AS hit_id
    , h.hitNumber AS hit_number
    , TIMESTAMP_ADD(s.created_at, INTERVAL h.time MILLISECOND) AS hit_at
    , COALESCE(h.isEntrance, FALSE) AS is_first
    , COALESCE(h.isExit, FALSE) AS is_last
    , COALESCE(h.isInteraction, FALSE) AS is_interaction
    , h.type
    , h.page.hostname AS host_name
    , h.page.pageTitle AS title
    , h.page.pagePath AS page_path
    , h.page.pagePathLevel1 AS page_path_level1
    , h.eventInfo.eventCategory
    , h.eventInfo.eventAction
    , h.eventInfo.eventLabel
  FROM rbo_sessions s
  CROSS JOIN UNNEST(hits) h
), rbo_events_transformed AS (
  SELECT created_date
    , created_at
    , client_id
    , session_id
    , visit_number
    , has_intraction
    , is_first_visit
    , is_bounce
    , total_hits
    , pageviews
    , time_on_site
    , browser
    , browser_size
    , mobile_device_info
    , mobile_device_marketing_name
    , mobile_device_model
    , mobile_input_selector
    , os
    , os_version
    , mobile_device_branding
    , language
    , screen_resolution
    , screen_colors
    , hit_id
    , hit_number
    , hit_at
    , is_first
    , is_last
    , is_interaction
    , type
    , host_name
    , title
    , eventCategory AS category
    , SPLIT(eventAction, '.')[SAFE_OFFSET(0)] AS action_object
    , SPLIT(eventAction, '.')[SAFE_OFFSET(1)] AS action_method
    , eventLabel AS label
    , page_path
    , CASE
        WHEN page_path_level1 = '/main'
          THEN 'main'
        WHEN page_path_level1 = '/login'
          THEN 'login'
        WHEN page_path_level1 = '/toto'
          THEN 'TOTO'
        WHEN page_path_level1 = '/device-management'
          THEN 'DMS'
        WHEN page_path_level1 = '/vendor-monitor'
          THEN 'Vendor Monitor'
        WHEN page_path_level1 = '/pos-menu-importer'
          THEN 'POS menu importer'
        ELSE page_path_level1
      END AS plugin
  FROM rbo_events
), rbo_events_aggreated AS (
  SELECT session_id
    , ARRAY_AGG(
        STRUCT(
          plugin
          , hit_number
          , hit_at
          , type
          , is_first
          , is_last
          , is_interaction
          , STRUCT(
              category AS name
              , action_object
              , action_method
              , label
              , STRUCT(
                  CAST(NULL AS STRING) AS delivery_platform
                  , CAST(NULL AS STRING) AS country_code
                  , CAST(NULL AS STRING) AS vendor_code
                  , CAST(NULL AS STRING) AS order_id
                  , CAST(NULL AS STRING) AS order_code
                  , CAST(NULL AS STRING) AS test_order_type
                  , CAST(NULL AS STRING) AS test_order_status
                ) AS toto
              , STRUCT(
                  CAST(NULL AS STRING) AS country_code
                  , CAST(NULL AS STRING) AS assign_type
                  , CAST(NULL AS STRING) AS device_id
                  , CAST(NULL AS INT64) AS vendor_id
                  , CAST(NULL AS STRING) AS vendor_name
                  , CAST(NULL AS INT64) AS bad_records
                ) AS dms
            ) AS event
          , STRUCT(
              title
              , host_name
              , page_path AS path
          ) AS hits_page
        )
      ) AS hits
  FROM rbo_events_transformed
  GROUP BY 1
)
SELECT s.created_date
  , s.created_at
  , IF(s.region = 'as', 'ap', region) AS region
  , CAST(NULL AS STRING) AS user_id
  , s.client_id
  , s.session_id
  , s.visit_number
  , s.is_first_visit
  , s.has_intraction
  , s.is_bounce
  , STRUCT(
      s.total_hits AS hits
      , s.pageviews
      , s.time_on_site
    ) AS total
  , STRUCT(
      s.os
      , s.os_version
      , s.browser
      , s.browser_size
      , s.language
      , s.screen_resolution
      , s.screen_colors
      , STRUCT(
          s.mobile_device_info AS info
          , s.mobile_device_branding AS branding
          , s.mobile_device_marketing_name AS marketing_name
          , s.mobile_device_model AS model
          , s.mobile_input_selector AS input_selector
        ) AS mobile
    ) AS device
  , ev.hits
FROM rbo_sessions s
INNER JOIN rbo_events_aggreated ev ON s.session_id = ev.session_id
