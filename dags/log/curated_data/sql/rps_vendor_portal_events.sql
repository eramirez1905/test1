CREATE TEMP FUNCTION extract_customdimensions(_index INT64, _properties ANY TYPE) AS (
  (SELECT value FROM UNNEST(_properties) WHERE index = _index)
);

WITH vendors AS (
  SELECT * EXCEPT (_row_number)
  FROM (
    SELECT v.entity_id
      , v.vendor_code
      , rps.region
      , rps.vendor_id
      , ROW_NUMBER() OVER (PARTITION BY entity_id, vendor_code, vendor_id ORDER BY rps.updated_at DESC) AS _row_number
    FROM `{{ params.project_id }}.cl.vendors_v2` v
    CROSS JOIN UNNEST(rps) rps
    WHERE rps.is_latest
  )
  WHERE _row_number = 1
), rps_portal_sessions AS (
  SELECT *
  FROM `{{ params.project_id }}.dl.rps_portal_sessions`
{%- if not params.backfill %}
  WHERE created_date BETWEEN DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY) AND '{{ next_ds }}'
{%- endif %}
), portal_session_data AS (
  SELECT s.*
    , entity_id_and_vendor_code
  FROM (
    SELECT created_date
      , TIMESTAMP_SECONDS(visitStartTime) AS created_at
      , COALESCE(userId, extract_customdimensions(1, customDimensions)) AS user_id
      , clientId AS client_id
      , CONCAT(fullVisitorId,'-',CAST(visitId AS STRING)) AS session_id
      , visitNumber AS visit_number
      , CAST(totals.visits IS NOT NULL AS BOOL) AS has_intraction
      , CAST(totals.newVisits IS NOT NULL AS BOOL) AS is_first_visit
      , CAST(totals.bounces IS NOT NULL AS BOOL) AS is_bounce
      , totals.hits AS total_hits
      , totals.pageviews
      , totals.timeOnSite
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
      , extract_customdimensions(4, customDimensions) AS country_code
      , extract_customdimensions(6, customDimensions) AS entity_ids
      , extract_customdimensions(7, customDimensions) AS entity_id_and_vendor_codes
      , hits
    FROM rps_portal_sessions
  ) s
  CROSS JOIN UNNEST(SPLIT(entity_id_and_vendor_codes, ',')) entity_id_and_vendor_code
), portal_event_data AS (
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
    , s.timeOnSite
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
    , s.user_id
    , s.country_code
    , extract_customdimensions(3, h.customDimensions) AS vendor_id
    , CONCAT(s.session_id, CAST(h.hitNumber AS STRING), type) AS hit_id
    , h.hitNumber AS number
    , TIMESTAMP_ADD(created_at, INTERVAL h.time MILLISECOND) AS hit_at
    , COALESCE(h.isEntrance, FALSE) AS is_first
    , COALESCE(h.isExit, FALSE) AS is_last
    , COALESCE(h.isInteraction, FALSE) AS is_interaction
    , h.type
    , extract_customdimensions(2, h.customDimensions) AS page_type
    , h.page.pagePath AS path
    , h.page.hostname AS host_name
    , h.page.pageTitle AS title
    , h.eventInfo.eventCategory
    , h.eventInfo.eventAction
    , h.eventInfo.eventLabel
  FROM portal_session_data s
  CROSS JOIN UNNEST(hits) h
), portal_event_data_transformed AS (
  SELECT created_date
    , created_at
    , user_id
    , client_id
    , session_id
    , visit_number
    , has_intraction
    , is_first_visit
    , is_bounce
    , total_hits
    , pageviews
    , timeOnSite
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
    , country_code
    , vendor_id
    , hit_id
    , number
    , hit_at
    , is_first
    , is_last
    , is_interaction
    , type
    , CASE
        WHEN type = 'PAGE'
            AND REGEXP_CONTAINS(page_type,'([0-9])\\w+')
          THEN 'individual_shop'
        WHEN type = 'PAGE'
            AND NOT REGEXP_CONTAINS(page_type,'([0-9])\\w+')
          THEN page_type
          ELSE NULL
      END AS page_type
    , path
    , host_name
    , title
    , CASE
        WHEN eventCategory LIKE 'menu-management%'
          THEN 'menu-management'
        WHEN eventCategory LIKE 'opening%times%'
          THEN 'opening-times'
          ELSE eventCategory
      END AS category
    , SPLIT(eventAction, '.')[SAFE_OFFSET(0)] AS action_object
    , SPLIT(eventAction, '.')[SAFE_OFFSET(1)] AS action_method
    , eventLabel AS label
  FROM portal_event_data
), portal_event_data_aggreated AS (
  SELECT session_id
    , ARRAY_AGG(
        STRUCT(
          vendor_id
          , number
          , hit_at
          , type
          , is_first
          , is_last
          , is_interaction
        )
      ) AS hits
    , ARRAY_AGG(
        STRUCT(
          category
          , action_object
          , action_method
          , label
        )
      ) AS hits_event
    , ARRAY_AGG(
        STRUCT(
          page_type AS type
          , title
          , host_name
          , path
        )
      ) AS hits_page
  FROM portal_event_data_transformed
  GROUP BY 1
), portal_data_aggregated AS (
  SELECT s.created_date
    , s.created_at
    , s.user_id
    , s.client_id
    , SPLIT(entity_id_and_vendor_code, ';')[OFFSET(0)] AS entity_id
    , SPLIT(entity_id_and_vendor_code, ';')[SAFE_OFFSET(1)] AS vendor_code
    , s.country_code
    , en.region_short_name AS region
    , s.session_id
    , s.visit_number
    , s.has_intraction
    , s.is_first_visit
    , s.is_bounce
    , STRUCT(
        s.total_hits AS hits
        , s.pageviews
        , s.timeOnSite
      ) AS total
    , STRUCT(
        s.browser
        , s.browser_size
        , s.mobile_device_info
        , s.mobile_device_branding
        , s.mobile_device_marketing_name
        , s.mobile_device_model
        , s.mobile_input_selector
        , s.os
        , s.os_version
        , s.language
        , s.screen_resolution
        , s.screen_colors
      ) AS device
    , ev.hits
    , ev.hits_event
    , ev.hits_page
  FROM portal_session_data s
  INNER JOIN portal_event_data_aggreated ev ON s.session_id = ev.session_id
  LEFT JOIN `{{ params.project_id }}.cl.entities` en ON s.country_code = LOWER(en.country_iso)
)
SELECT s.created_date
  , s.created_at
  , s.user_id
  , s.client_id
  , v.vendor_id
  , s.entity_id
  , s.vendor_code
  , s.country_code
  , s.region
  , s.session_id
  , s.visit_number
  , s.has_intraction
  , s.is_first_visit
  , s.is_bounce
  , s.total
  , s.device
  , s.hits
  , s.hits_event
  , s.hits_page
FROM portal_data_aggregated s
LEFT JOIN vendors v ON s.vendor_code = v.vendor_code
    AND s.entity_id = v.entity_id
