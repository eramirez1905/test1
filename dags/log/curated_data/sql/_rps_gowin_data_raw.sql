CREATE TEMP FUNCTION extract_gowin_params(_index INT64, _properties ANY TYPE) AS (
  (SELECT value FROM UNNEST(_properties) WHERE index = _index)
);

CREATE TEMP FUNCTION clean_params(_params STRING) AS (
  (IF(_params <> 'NA', _params, NULL))
);

WITH entities AS (
  SELECT en.region_short_name AS region
    , p.entity_id
    , p.display_name
    , p.is_active
    , p.timezone
    , rps AS platform_key
    , en.country_iso
  FROM `{{ params.project_id }}.cl.entities` en
  CROSS JOIN UNNEST(platforms) p
  CROSS JOIN UNNEST(rps_platforms) rps
  WHERE region_short_name IS NOT NULL
), valid_entity_id AS (
  SELECT DISTINCT entity_id
    , timezone
  FROM entities
), countries AS (
  SELECT DISTINCT region_short_name AS region
    , country_iso
  FROM `{{ params.project_id }}.cl.countries`
), vendors AS (
  SELECT DISTINCT v.entity_id
    , v.vendor_code
    , rps.timezone
    , rps.vendor_id
    , rps.operator_code AS _operator_code
    , rps.rps_global_key
    , rps.country_iso
    , rps.region
    , rps.is_active
    , rps.updated_at
  FROM `{{ params.project_id }}.cl.vendors_v2` v
  CROSS JOIN UNNEST(rps) rps
  WHERE rps.is_latest
), go_win_sessions_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.dl.go_win_sessions` s
{%- if not params.backfill %}
  WHERE created_date BETWEEN DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY) AND '{{ next_ds }}'
{%- endif %}
), go_win_events AS (
  SELECT CAST(TIMESTAMP_ADD(TIMESTAMP_SECONDS(s.visitStartTime), INTERVAL h.time MILLISECOND) AS DATE) AS created_date
    , TIMESTAMP_ADD(TIMESTAMP_SECONDS(s.visitStartTime), INTERVAL h.time MILLISECOND) AS created_at
    , h.hitNumber
    , h.eventInfo.eventAction AS event_name
    , h.eventInfo.eventLabel AS event_label
    , CAST(h.eventInfo.eventValue AS STRING) AS event_value
    , clean_params(extract_gowin_params(1, h.customDimensions)) AS _operator_code
    , clean_params(extract_gowin_params(2, h.customDimensions)) AS _operator_code_user
    , clean_params(extract_gowin_params(37, h.customDimensions)) AS vendor_id
    , extract_gowin_params(8, h.customDimensions) AS vendor_code
    , clean_params(extract_gowin_params(18, h.customDimensions)) AS platform_key
    , UPPER(extract_gowin_params(17, h.customDimensions)) AS country_iso
    , extract_gowin_params(11, h.customDimensions) AS client_version
    , extract_gowin_params(10, h.customDimensions) AS client_wrapper
    , clean_params(extract_gowin_params(12, h.customDimensions)) AS client_wrapper_version
    , extract_gowin_params(16, h.customDimensions) AS vendor_status
    , LOWER(extract_gowin_params(21, h.customDimensions)) AS order_id
    , extract_gowin_params(20, h.customDimensions) AS order_code
    , extract_gowin_params(13, h.customDimensions) AS delivery_type
    , extract_gowin_params(19, h.customDimensions) AS is_preorder
    , extract_gowin_params(35, h.customDimensions) AS delayed_minutes
    , clean_params(extract_gowin_params(24, h.customDimensions)) AS minutes
    , clean_params(extract_gowin_params(14, h.customDimensions)) AS accepted_minutes
    , clean_params(extract_gowin_params(25, h.customDimensions)) AS reason
    , extract_gowin_params(23, h.customDimensions) AS reject_reason
    , extract_gowin_params(26, h.customDimensions) AS cancellation_reason
    , extract_gowin_params(31, h.customDimensions) AS order_payment_method
    , extract_gowin_params(33, h.customDimensions) AS login_type
    , extract_gowin_params(3, h.customDimensions) AS product_id
    , extract_gowin_params(4, h.customDimensions) AS product_name
    , extract_gowin_params(7, h.customDimensions) AS product_availability
    , LOWER(extract_gowin_params(29, h.customDimensions)) AS connectivity_dismissed_by
    , extract_gowin_params(28, h.customDimensions) AS printer_printing_event_type
    , extract_gowin_params(9, h.customDimensions) AS auxiliary_order_history_search_query
    , extract_gowin_params(27, h.customDimensions) AS auxiliary_order_history_search_result
    , clean_params(extract_gowin_params(30, h.customDimensions)) AS auxiliary_sound_type
    , PARSE_DATE('%Y%m%d', date) AS session_start_date
    , TIMESTAMP_SECONDS(s.visitStartTime) AS session_start_at
    , s.clientId AS client_id
    , CONCAT(fullVisitorId, '-' , CAST(s.visitId AS STRING)) AS session_id
    , extract_gowin_params(36, h.customDimensions) AS device_id
    , s.device.operatingSystem AS device_os
    , s.device.operatingSystemVersion AS device_os_version
    , s.device.mobileDeviceBranding AS device_model
    , s.device.mobileDeviceMarketingName AS device_brand
  FROM go_win_sessions_dataset s
  CROSS JOIN UNNEST(hits) h
  WHERE h.eventInfo.eventAction <> 'by-time'
), go_win_events_clean AS (
  SELECT e.created_date
    , e.created_at
    , e.hitNumber
    , ev.event_name
    , e.event_name AS _original_event_name
    , ev.event_scope
    , e.event_label
    , e.event_value
    , TRIM(COALESCE(e._operator_code, e._operator_code_user)) AS _operator_code
    , SAFE_CAST(e.vendor_id AS INT64) AS vendor_id
    , e.vendor_code
    , IF(e.event_name NOT IN ('login_started', 'login.succeeded', 'login.failed', 'logout.submitted', 'connectivity_warning_dismissed'), e.platform_key, NULL) AS platform_key
    , e.country_iso
    , co.region
    , e.client_version
    , e.client_wrapper
    , e.client_wrapper_version
    , CASE
        WHEN e.vendor_status IN ('CLOSED', 'CLOSED_UNTIL', 'CLOSED_TODAY')
          THEN 'UNAVAILABLE'
        WHEN e.vendor_status = 'OPEN'
          THEN 'AVAILABLE'
        ELSE NULL
      END AS vendor_status
    , CASE
        WHEN e.vendor_status = 'CLOSED_TODAY'
          THEN 'CURRENT_DAY'
        WHEN e.vendor_status = 'CLOSED_UNTIL'
          THEN 'TIME_LIMITED'
        WHEN e.vendor_status = 'CLOSED'
          THEN 'CURRENT_DAY'
        ELSE NULL
      END AS vendor_unavailable_duration
    , e.order_id
    , e.order_code
    , CASE
        WHEN e.delivery_type = 'PICKUP_LOGISTICS'
          THEN 'OWN_DELIVERY'
        WHEN e.delivery_type = 'RESTAURANT_DELIVERY'
          THEN 'VENDOR_DELIVERY'
        WHEN e.delivery_type = 'CUSTOMER_PICKUP'
          THEN 'PICKUP'
        ELSE NULL
      END AS delivery_type
    , e.is_preorder
    , ev.order_status
    , e.order_payment_method
    -- This cleaning logic applied to deal with excessively collected data coming from GA/GTM. The code of the application itself is not tracking this piece of information
    , IF(ev.is_fail_order_status, COALESCE(e.reason, e.reject_reason, e.cancellation_reason), NULL) AS order_cancelled_reason
    , CAST(COALESCE(e.event_value, e.minutes, e.accepted_minutes, e.delayed_minutes) AS INT64) AS minutes
    , UPPER(e.login_type) AS login_type
    , e.product_id
    , e.product_name
    , CASE
        WHEN product_availability = 'STATUS_ACTIVE'
          THEN TRUE
        WHEN product_availability IN ('STATUS_INACTIVE', 'STATUS_INACTIVE_UNTIL_TOMORROW')
          THEN FALSE
        ELSE NULL
      END AS product_availability
    , CASE
        WHEN product_availability = 'STATUS_ACTIVE'
          THEN NULL
        WHEN product_availability = 'STATUS_INACTIVE_UNTIL_TOMORROW'
          THEN 'TODAY'
        WHEN product_availability = 'STATUS_INACTIVE'
          THEN 'INDEFINITELY'
        ELSE NULL
      END AS product_availability_duration
    , e.connectivity_dismissed_by
    , e.printer_printing_event_type
    , e.auxiliary_order_history_search_query
    , e.auxiliary_order_history_search_result
    , COALESCE(e.auxiliary_sound_type, 'Default') AS auxiliary_sound_type
    , e.session_start_date
    , e.session_start_at
    , e.client_id
    , e.session_id
    , e.device_id
    , IF(e.device_os <> '(not set)', e.device_os, NULL) AS device_os
    , IF(e.device_os_version <> '(not set)', e.device_os_version, NULL) AS device_os_version
    , IF(SUBSTR(e.device_id, 0, 2) = 'V2', 'Sunmi', e.device_brand) AS device_brand
    , IF(SUBSTR(e.device_id, 0, 2) = 'V2', 'V2 PRO', e.device_model) AS device_model
    , SPLIT(e.platform_key, '_')[OFFSET(0)] AS _platform_brand_id
    , ev.is_platform_specific
    , CONCAT(e.session_id, '-', CAST(e.hitNumber AS STRING)) AS event_id
  FROM go_win_events e
  LEFT JOIN `{{ params.project_id }}.cl._rps_vendor_client_events_normalized` ev ON e.event_name = ev.gowin_event_name_raw
  LEFT JOIN countries co ON e.country_iso = co.country_iso
), go_win_events_with_entity_id AS (
  SELECT e.created_date
    , e.created_at
    , e.event_name
    , e._original_event_name
    , e.event_scope
    , e._operator_code
    , e.vendor_id
    , COALESCE(e.vendor_code, v2.vendor_code) AS vendor_code
    , e.platform_key
    , COALESCE(v1.entity_id, v2.entity_id, CONCAT(e._platform_brand_id, '_', e.country_iso)) AS entity_id
    , IF(e.country_iso <> 'na', e.country_iso, NULL) AS country_iso
    , e.region
    , e.order_id
    , e.order_code
    , e.delivery_type
    , e.is_preorder = '1' AS is_preorder
    , COALESCE(STARTS_WITH(e.order_code, 'TEST') OR STARTS_WITH(e.order_code, 'TOTO'), FALSE) AS is_test_order
    , e.order_status
    -- Mapping of the legacy cancellation reasons
    , CASE
        WHEN order_cancelled_reason IN ('UPDATES_IN_MENU', 'MENU_ACCOUNT_PROBLEM')
          THEN 'MENU_ACCOUNT_SETTINGS'
        WHEN order_cancelled_reason = 'INCOMPLETE_ADDRESS'
          THEN 'ADDRESS_INCOMPLETE_MISSTATED'
        WHEN order_cancelled_reason = 'MODIFICATION_NOT_POSSIBLE'
          THEN 'ORDER_MODIFICATION_NOT_POSSIBLE'
        ELSE e.order_cancelled_reason
      END AS order_cancelled_reason
    , IF(e.delivery_type = 'VENDOR_DELIVERY', e.minutes, NULL) AS order_minutes
    , e.order_payment_method
    , e.product_id
    , e.product_name
    , e.product_availability
    , e.product_availability_duration
    , 'GOWIN' AS client_name
    , e.client_version
    , e.client_wrapper
    , e.client_wrapper_version
    , e.device_os
    , e.device_os_version
    , e.device_brand
    , e.device_model
    , e.device_id
    , e.connectivity_dismissed_by
    , e.printer_printing_event_type
    , e.vendor_status
    , e.vendor_unavailable_duration
    , IF(e.vendor_unavailable_duration = 'TIME_LIMITED', e.minutes, NULL) AS vendor_minutes
    , e.login_type
    , e.auxiliary_order_history_search_query
    , e.auxiliary_order_history_search_result
    , e.auxiliary_sound_type
  FROM go_win_events_clean e
  LEFT JOIN vendors v1 ON e.vendor_code = v1.vendor_code
    AND e.country_iso = v1.country_iso
    AND e.platform_key = v1.rps_global_key
    AND e.region = v1.region
    AND e.is_platform_specific
  LEFT JOIN vendors v2 ON e._operator_code = v2._operator_code
    AND e.region = v2.region
    AND NOT e.is_platform_specific
  WHERE ((e.is_platform_specific AND v1.vendor_code IS NOT NULL)
    OR (NOT e.is_platform_specific AND v2.vendor_id IS NOT NULL)
    OR e.is_platform_specific IS NULL)
)
SELECT e.*
  , en.timezone
FROM go_win_events_with_entity_id e
INNER JOIN valid_entity_id en ON e.entity_id = en.entity_id
