CREATE TEMP FUNCTION extract_godroid_params(_key STRING, _properties ANY TYPE) AS (
  (SELECT value.string_value FROM UNNEST(_properties) WHERE key = _key)
);

CREATE TEMP FUNCTION clean_params_coalesce(_key STRING, _properties1 ANY TYPE, _properties2 ANY TYPE) AS (
  (COALESCE(extract_godroid_params(_key, _properties1), extract_godroid_params(_key, _properties2)))
);

CREATE TEMP FUNCTION clean_next_opening_at(_dirty_checkin_next_opening_time STRING) AS (
  SAFE.PARSE_TIMESTAMP(
    '%Y-%b-%d %T'
    , (CONCAT(
        SPLIT(_dirty_checkin_next_opening_time, ' ')[SAFE_OFFSET(5)] -- Year
        , '-'
        , SPLIT(_dirty_checkin_next_opening_time, ' ')[SAFE_OFFSET(1)] -- Month
        , '-'
        , SPLIT(_dirty_checkin_next_opening_time, ' ')[SAFE_OFFSET(2)] -- Day
        , ' '
        , SPLIT(_dirty_checkin_next_opening_time, ' ')[SAFE_OFFSET(3)] -- Time
        )
      )
    , SUBSTR(
          SPLIT(
            SPLIT(
              _dirty_checkin_next_opening_time
              , ' '
            )[SAFE_OFFSET(4)] -- Timezone
          , ':')[SAFE_OFFSET(0)] -- The timezone offset hour
        , 4)
  )
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
), rps_vendors AS (
  SELECT *
  FROM (
    SELECT region
      , vendor_id
      , entity_id
      , vendor_code
      , ROW_NUMBER() OVER (PARTITION BY region, vendor_id ORDER BY updated_at DESC) AS _row_number
    FROM vendors
  )
  WHERE _row_number = 1
), go_droid_events_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.dl.go_droid_events`
{%- if not params.backfill %}
  WHERE created_date BETWEEN DATE_SUB('{{ next_ds }}', INTERVAL 3 DAY) AND '{{ next_ds }}'
{%- endif %}
), go_droid_events_raw AS (
  SELECT created_date
    , created_at
    , SAFE_CAST(extract_godroid_params('event_timestamp_client', event_params) AS INT64) AS event_unix_timestamp_client
    , UPPER(clean_params_coalesce('country_code', user_properties, event_params)) AS country_iso
    , event_name
    , clean_params_coalesce('platform_key', user_properties, event_params) AS platform_key
    , SAFE_CAST(clean_params_coalesce('vendor_id', user_properties, event_params) AS INT64) AS vendor_id
    , extract_godroid_params('platform_restaurant_id', event_params) AS vendor_code
    , LOWER(extract_godroid_params('operator_code', user_properties)) AS _operator_code
    , COALESCE(extract_godroid_params('device_id', user_properties), extract_godroid_params('device_id', event_params)) AS device_id
    , clean_params_coalesce('restaurant_status', user_properties, event_params) AS vendor_status
    , extract_godroid_params('availability', event_params) AS vendor_availability
    , LOWER(extract_godroid_params('order_id', event_params)) AS order_id
    , extract_godroid_params('platform_order_id', event_params) AS platform_order_code
    , extract_godroid_params('order_code', event_params) AS order_code
    , extract_godroid_params('delivery_type', event_params) AS delivery_type
    , extract_godroid_params('is_preorder', event_params) AS is_preorder
    , extract_godroid_params('decline_reason', event_params) AS decline_reason
    , extract_godroid_params('not_picked_up_reason', event_params) AS not_picked_up_reason
    , extract_godroid_params('selected_delivery_time_mins', event_params) AS selected_delivery_time_mins
    , extract_godroid_params('promised_delivery_time_mins', event_params) AS promised_delivery_time_mins
    , extract_godroid_params('product_id', event_params) AS product_id
    , extract_godroid_params('product_ids', event_params) AS product_ids
    , extract_godroid_params('product_name', event_params) AS product_name
    , extract_godroid_params('available', event_params) AS product_availability
    , extract_godroid_params('product_availability', event_params) AS product_availability_menu_search
    , extract_godroid_params('dismissed_by', event_params) AS connectivity_dismissed_by
    , extract_godroid_params('network_type', event_params) AS connectivity_network_type
    , extract_godroid_params('new_orders_count', event_params) AS auxiliary_new_orders_count
    , extract_godroid_params('connectionTime', event_params) AS printer_connection_duration
    , extract_godroid_params('printerName', event_params) AS printer_name
    , extract_godroid_params('printerType', event_params) AS printer_type
    , extract_godroid_params('printingTime', event_params) AS printer_printing_duration
    , extract_godroid_params('receiptsNumber', event_params) AS printer_receipts_number
    , extract_godroid_params('previous_locale', event_params) AS auxiliary_previous_locale
    , extract_godroid_params('new_locale', event_params) AS auxiliary_new_locale
    , extract_godroid_params('search_query', event_params) AS menu_search_query
    , extract_godroid_params('search_type', event_params) AS menu_search_type
    , extract_godroid_params('search_results_count', event_params) AS menu_search_results_count
    , extract_godroid_params('product_ids_shown', event_params) AS menu_product_ids_shown
    , extract_godroid_params('product_ids_selected', event_params) AS menu_product_ids_selected
    , extract_godroid_params('from', event_params) AS auxiliary_onboarding_from
    , extract_godroid_params('page', event_params) AS auxiliary_onboarding_page
    , extract_godroid_params('action', event_params) AS auxiliary_onboarding_action
    , extract_godroid_params('tutorial_id', event_params) AS auxiliary_tutorial_name
    , extract_godroid_params('menu_item', event_params) AS auxiliary_navigation_menu_item
    , extract_godroid_params('notification_displayed', event_params) AS auxiliary_notification_displayed
    , extract_godroid_params('ringtone', event_params) AS auxiliary_ringtone
    , extract_godroid_params('volume', event_params) AS auxiliary_volume
    , extract_godroid_params('context', event_params) AS auxiliary_context
    , extract_godroid_params('availability_status', event_params) AS checkin_availability_status
    , extract_godroid_params('next_opening_time', event_params) AS checkin_next_opening_time
    , extract_godroid_params('reason', event_params) AS auxiliary_device_restarted_reason
    , UPPER(extract_godroid_params('country_code', event_params)) AS login_selected_country_iso
    , extract_godroid_params('experiments', user_properties) AS experiments
    , app_info.version
    , device.operating_system AS device_os
    , device.operating_system_version AS device_os_version
    , device.mobile_brand_name AS device_brand
    , device.mobile_marketing_name AS device_model
  FROM go_droid_events_dataset
  WHERE event_name NOT IN ('ad_click', 'ad_exposure', 'ad_impression', 'ad_query', 'ad_reward', 'adunit_exposure'
      , 'app_clear_data', 'app_remove', 'app_update', 'dynamic_link_app_open', 'dynamic_link_app_update', 'dynamic_link_first_open'
      , 'first_open', 'in_app_purchase', 'notification_dismiss', 'notification_foreground', 'notification_open'
      , 'notification_receive', 'os_update', 'screen_view', 'session_start', 'user_engagement', 'error', 'get_order_api'
      , 'get_orders_api', 'push_notification_received', 'menu_item_component_ack')
    -- Get rid of the ack events as they are not a good indicator to measure user activity with the application,
    -- except for `ack_set_product_availability` as this is the only event to measure Product Unavailability
    AND (event_name NOT LIKE 'ack_%'
      OR event_name = 'ack_set_product_availability')
), go_droid_events_clean AS (
  SELECT e.created_date
    , e.created_at
    , ev.event_name
    , e.event_name AS _original_event_name
    , ev.event_scope
    , e._operator_code
    , e.vendor_id
    , e.vendor_code
    , CASE
        WHEN COALESCE(e.vendor_availability, e.vendor_status) IN ('BUSY', 'UNAVAILABLE')
          THEN 'UNAVAILABLE'
        WHEN COALESCE(e.vendor_availability, e.vendor_status) = 'AVAILABLE'
          THEN 'AVAILABLE'
        ELSE COALESCE(e.vendor_availability, e.vendor_status)
      END AS vendor_status
    , CASE
        WHEN COALESCE(e.vendor_availability, e.vendor_status) = 'BUSY'
          THEN 'TIME_LIMITED'
        WHEN COALESCE(e.vendor_availability, e.vendor_status) = 'UNAVAILABLE'
          THEN 'CURRENT_DAY'
        ELSE NULL
      END AS vendor_unavailable_duration
    , IF(ev.event_name = 'vendor_status_changed' AND COALESCE(e.vendor_availability, e.vendor_status) = 'BUSY', 30, NULL) AS vendor_minutes
    , IF(e.platform_key <> 'NA', e.platform_key, NULL) AS platform_key
    , co.region
    , e.country_iso
    , e.order_id
    , COALESCE(e.platform_order_code, e.order_code) AS order_code
    , e.is_preorder
    , CASE
        WHEN e.delivery_type = 'PICKUP_LOGISTICS'
          THEN 'OWN_DELIVERY'
        WHEN e.delivery_type = 'RESTAURANT_DELIVERY'
          THEN 'VENDOR_DELIVERY'
        WHEN e.delivery_type = 'CUSTOMER_PICKUP'
          THEN 'PICKUP'
        ELSE NULL
      END AS delivery_type
    , ev.order_status
    , COALESCE(e.decline_reason, e.not_picked_up_reason) AS order_cancelled_reason
    , CAST(promised_delivery_time_mins AS INT64) AS order_minutes_suggested
    , CAST(selected_delivery_time_mins AS INT64) AS order_minutes
    , COALESCE(e.product_id, TRIM(product_ids)) AS product_id
    , e.product_name
    , CASE
        WHEN e.event_name = 'user_product_enable' OR product_availability_menu_search = 'available'
          THEN TRUE
        WHEN e.product_availability = 'false'
            OR e.event_name IN ('user_product_disable', 'ack_set_product_availability', 'user_product_disable_indefinitely')
            OR product_availability_menu_search IN ('unavailable_today', 'unavailable_indefinitely')
          THEN FALSE
        ELSE NULL
      END AS product_availability
    , CASE
        WHEN e.event_name = 'user_product_disable_indefinitely' OR product_availability_menu_search = 'unavailable_indefinitely'
          THEN 'INDEFINITELY'
        WHEN e.event_name IN ('user_product_disable', 'ack_set_product_availability', 'user_product_enable', 'order_decline_set_item_unavailable')
              OR product_availability_menu_search = 'unavailable_today'
          THEN 'TODAY'
      END AS product_availability_duration
    -- This logic is applied here to handle the bug of ROD-1221
    , COALESCE(SAFE.TIMESTAMP_MILLIS(e.event_unix_timestamp_client), TIMESTAMP_MICROS(e.event_unix_timestamp_client)) AS event_timestamp_client
    , e.menu_search_query
    , e.menu_search_type
    , CAST(e.menu_search_results_count AS INT64) AS menu_search_results_count
    , e.menu_product_ids_shown
    , e.menu_product_ids_selected
    , e.auxiliary_previous_locale
    , e.auxiliary_new_locale
    , CASE
        WHEN e.auxiliary_onboarding_from = 'INIT'
          THEN 'INITIATION'
        WHEN e.auxiliary_onboarding_from = 'NAV'
          THEN 'NAVIGATION'
        ELSE NULL
      END AS auxiliary_onboarding_from
    , SAFE_CAST(e.auxiliary_onboarding_page AS INT64) AS auxiliary_onboarding_page
    , e.auxiliary_onboarding_action
    , e.auxiliary_tutorial_name
    , e.auxiliary_navigation_menu_item
    , CAST(e.auxiliary_notification_displayed AS BOOL) AS auxiliary_notification_displayed
    , e.login_selected_country_iso
    , e.experiments
    , e.version
    , e.device_os
    , e.device_os_version
    , IF(SUBSTR(e.device_id, 0, 2) = 'V2', 'Sunmi', e.device_brand) AS device_brand
    , IF(SUBSTR(e.device_id, 0, 2) = 'V2', 'V2 PRO', e.device_model) AS device_model
    , e.device_id
    , e.connectivity_dismissed_by
    , e.connectivity_network_type
    , (CAST(e.printer_connection_duration AS INT64) / 1000) AS printer_connection_duration
    , e.printer_name
    , IF(e.printer_type <> 'UNKNOWN', e.printer_type, NULL) AS printer_type
    , (CAST(e.printer_printing_duration AS INT64) / 1000) AS printer_printing_duration
    , CAST(e.printer_receipts_number AS INT64) AS printer_receipts_number
    , e.checkin_availability_status
    , clean_next_opening_at(e.checkin_next_opening_time) AS checkin_next_opening_at
    , CAST(e.auxiliary_new_orders_count AS INT64) AS auxiliary_new_orders_count
    , IF(e.auxiliary_ringtone = 'Default', e.auxiliary_ringtone, 'Custom') AS auxiliary_ringtone
    , e.auxiliary_volume
    , CASE
        WHEN e.auxiliary_context = 'INIT'
          THEN 'INITIATION'
        WHEN e.auxiliary_context = 'NAV'
          THEN 'NAVIGATION'
        ELSE NULL
      END AS auxiliary_context
    , e.auxiliary_device_restarted_reason
    , SPLIT(e.platform_key, '_')[OFFSET(0)] AS _platform_brand_id
    , ev.is_platform_specific
  FROM go_droid_events_raw e
  LEFT JOIN `{{ params.project_id }}.cl._rps_vendor_client_events_normalized` ev ON e.event_name = ev.godroid_event_name_raw
  LEFT JOIN UNNEST(SPLIT(e.product_ids)) product_ids
  LEFT JOIN countries co ON e.country_iso = co.country_iso
), daily_unique_vendors_code AS (
  SELECT * EXCEPT(vendor_codes)
  FROM (
    SELECT created_date
      , country_iso
      , vendor_id
      , vendor_code
      , COUNT(DISTINCT vendor_code) OVER (PARTITION BY created_date, country_iso, vendor_id) AS vendor_codes
    FROM go_droid_events_clean
    WHERE vendor_id IS NOT NULL
      AND vendor_code IS NOT NULL
    GROUP BY 1,2,3,4
    )
  WHERE vendor_codes = 1
), go_droid_events_clean_with_vendor_code AS (
  SELECT e.* EXCEPT(vendor_code)
     , COALESCE(e.vendor_code, uvc.vendor_code) AS vendor_code
  FROM go_droid_events_clean e
  LEFT JOIN daily_unique_vendors_code uvc ON e.created_date = uvc.created_date
    AND e.vendor_id = uvc.vendor_id
    AND e.country_iso = uvc.country_iso
), go_droid_events_with_entity_id AS (
  SELECT COALESCE(CAST(e.event_timestamp_client AS DATE), e.created_date) AS created_date
    , COALESCE(e.event_timestamp_client, e.created_at) AS created_at
    , e.event_name
    , e._original_event_name
    , e.event_scope
    , e.experiments
    , COALESCE(e._operator_code, v1._operator_code) AS _operator_code
    , e.vendor_id
    , COALESCE(e.vendor_code, v2.vendor_code) AS vendor_code
    , e.platform_key
    , COALESCE(v1.entity_id, v2.entity_id, CONCAT(e._platform_brand_id, '_', e.country_iso)) AS entity_id
    , e.country_iso
    , e.region
    , e.order_id
    , e.order_code
    , e.delivery_type
    , CAST(e.is_preorder AS BOOL) AS is_preorder
    , COALESCE(STARTS_WITH(e.order_code, 'TEST') OR STARTS_WITH(e.order_code, 'TOTO'), FALSE) AS is_test_order
    , e.order_status
    , e.order_cancelled_reason AS cancelled_reason
    , IF(e.delivery_type = 'VENDOR_DELIVERY', e.order_minutes_suggested, NULL) AS order_minutes_suggested
    , IF(e.delivery_type = 'VENDOR_DELIVERY', e.order_minutes, NULL) AS order_minutes
    , e.product_id
    , e.product_name
    , e.product_availability
    , e.product_availability_duration
    , 'GODROID' AS client_name
    , e.version AS client_version
    , e.device_os
    , e.device_os_version
    , e.device_brand
    , e.device_model
    , e.device_id
    , e.connectivity_network_type
    , e.connectivity_dismissed_by
    , e.vendor_status
    , e.vendor_unavailable_duration
    , e.vendor_minutes
    , e.printer_connection_duration
    , e.printer_name
    , e.printer_type
    , e.printer_receipts_number
    , e.printer_printing_duration
    , e.menu_search_query
    , e.menu_search_type
    , e.menu_search_results_count
    , CAST(e.menu_product_ids_shown AS BOOL) AS menu_is_products_shown
    , CAST(e.menu_product_ids_selected AS BOOL) AS menu_is_products_selected
    , e.checkin_availability_status
    , e.checkin_next_opening_at
    , e.auxiliary_new_orders_count
    , e.auxiliary_previous_locale
    , e.auxiliary_new_locale
    , e.auxiliary_tutorial_name
    , e.auxiliary_onboarding_from
    , e.auxiliary_onboarding_page
    , e.auxiliary_onboarding_action
    , e.auxiliary_navigation_menu_item
    , e.auxiliary_notification_displayed
    , e.auxiliary_ringtone
    , e.auxiliary_volume
    , e.auxiliary_context
    , e.auxiliary_device_restarted_reason
    , e.login_selected_country_iso
    , e.is_platform_specific
  FROM go_droid_events_clean_with_vendor_code e
  LEFT JOIN vendors v1 ON e.vendor_code = v1.vendor_code
    AND e.platform_key = v1.rps_global_key
    AND e.country_iso = v1.country_iso
    AND e.region = v1.region
    -- Temp solution until VES-51 is done
    AND e.event_scope = 'order'
  LEFT JOIN rps_vendors v2 ON e.vendor_id = v2.vendor_id
    AND e.region = v2.region
    -- Temp solution until VES-51 is done
    AND e.event_scope != 'order'
  WHERE ((e.event_scope = 'order' AND v1.vendor_code IS NOT NULL)
    OR (e.event_scope != 'order' AND v2.vendor_id IS NOT NULL)
    OR e.is_platform_specific IS NULL)
)
SELECT e.*
  , en.timezone
FROM go_droid_events_with_entity_id e
INNER JOIN valid_entity_id en ON e.entity_id = en.entity_id
