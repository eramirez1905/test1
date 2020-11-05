CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.rps_gsr_client_dashboard_events_hourly`
PARTITION BY created_date
CLUSTER BY entity_id, region, brand_name, country_name AS
WITH entities AS (
  SELECT en.region_short_name AS region
    , en.country_iso
    , en.country_name
    , p.entity_id
    , p.brand_name
    , p.timezone
  FROM `{{ params.project_id }}.cl.entities` en
  CROSS JOIN UNNEST(platforms) p
), rps_orders_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl.rps_orders`
  WHERE created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 3 MONTH)
), rps_orders AS (
  SELECT created_at
    , region
    , order_id
    , entity.id AS entity_id
    , client.name AS client_name
    , client.version AS client_version
    , COALESCE(client.wrapper_type, '(N/A)') AS client_wrapper_type
    , COALESCE(client.wrapper_version, '(N/A)') AS client_wrapper_version
    , order_status = 'cancelled' AS is_fail
    , cancellation.reason AS cancellation_reason
    , cancellation.owner AS cancellation_owner
  FROM rps_orders_dataset
  WHERE client.name IN ('GODROID', 'GOWIN')
), rps_orders_agg AS (
  SELECT TIMESTAMP_TRUNC(CAST(DATETIME(o.created_at, en.timezone) AS TIMESTAMP), HOUR) AS created_hour
    , o.entity_id
    , o.client_name
    , o.client_version
    , o.client_wrapper_type
    , o.client_wrapper_version
    , COUNT(DISTINCT o.order_id) AS orders_ct
    , COUNT(DISTINCT IF(o.is_fail, o.order_id, NULL)) AS fail_orders_ct
    , COUNT(DISTINCT IF(o.is_fail AND o.cancellation_owner = 'VENDOR', order_id, NULL)) AS fail_orders_vendor_ct
    , COUNT(DISTINCT IF(o.is_fail AND o.cancellation_reason = 'ITEM_UNAVAILABLE', order_id, NULL)) AS fail_orders_product_unavailable_ct
    , COUNT(DISTINCT IF(o.is_fail AND o.cancellation_reason = 'NO_RESPONSE', order_id, NULL)) AS fail_orders_no_response_ct
    , COUNT(DISTINCT IF(o.is_fail AND o.cancellation_reason = 'TOO_BUSY', order_id, NULL)) AS fail_orders_too_busy_ct
    , COUNT(DISTINCT IF(o.is_fail AND o.cancellation_reason = 'CLOSED', order_id, NULL)) AS fail_orders_closed_ct
  FROM rps_orders o
  INNER JOIN entities en ON o.entity_id = en.entity_id
  GROUP BY 1,2,3,4,5,6
), rps_vendor_client_events_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl.rps_vendor_client_events`
  WHERE created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 3 MONTH)
), rps_vendor_first_test_order AS(
  SELECT region
    , vendor_id
    , MIN(created_at) AS first_test_order_sent_at
  FROM rps_vendor_client_events_dataset
  WHERE event_name = 'tutorial_onboarding_test_order_button_clicked'
  GROUP BY 1, 2
), rps_vendor_client_events AS (
  SELECT TIMESTAMP_TRUNC(CAST(DATETIME(ev.created_at, en.timezone) AS TIMESTAMP), HOUR) AS created_hour
    , ev.region
    , ev.entity_id
    , ev.client.name AS client_name
    , ev.client.version AS client_version
    , COALESCE(ev.client.wrapper_type, '(N/A)') AS client_wrapper_type
    , COALESCE(ev.client.wrapper_version, '(N/A)') AS client_wrapper_version
    , CONCAT(ev.region , '-', ev.vendor_id ) AS uniqule_vendor_id
    , CONCAT(ev.entity_id , '-', ev.vendor_code ) AS uniqule_vendor_code
    , ev.orders.id AS order_id
    , ev.event_name
    , IF(ev.event_name = 'tutorial_onboarding_test_order_button_clicked', ev.created_at, NULL) AS test_order_sent_at
    , fto.first_test_order_sent_at
    , ev.product.id AS product_id
    , ev.product.availability AS product_availability
    , ev.product.availability_duration AS product_availability_duration
    , ev.vendor.status AS vendor_status
    , ev.vendor.unavailable_duration AS vendor_unavailable_duration
    , ev.vendor.minutes AS vendor_minutes
    , ev.login.type AS login_type
    , ev.auxiliary.sound_type AS sound_type
    , ev.auxiliary.sound_volume AS sound_volume
    , ev.printer.type AS printer_type
    , ev.printer.print_receipt.receipts_number AS print_receipts_number
    , ev.printer.print_receipt.printing_duration
    , ev.connectivity.network_type
    , ev.connectivity.dismissed_by
    , ev.auxiliary.new_orders_count
    , ev.auxiliary.onboarding_page
    , ev.auxiliary.onboarding_action
    , ev.auxiliary.onboarding_from
  FROM rps_vendor_client_events_dataset ev
  LEFT JOIN rps_vendor_first_test_order fto ON ev.vendor_id = fto.vendor_id
    AND ev.region = fto.region
  INNER JOIN entities en ON ev.entity_id = en.entity_id
  WHERE (event_scope IN ('vendor', 'product', 'printer', 'connectivity', 'login', 'auxiliary')
    OR orders.id IS NOT NULL)
), rps_client_dashboard_events_agg AS (
  SELECT created_hour
    , entity_id
    , client_name
    , client_version
    , client_wrapper_type
    , client_wrapper_version
    , COUNT(DISTINCT uniqule_vendor_id) AS vendor_id_ct
    , COUNT(DISTINCT uniqule_vendor_code) AS vendor_code_ct
    , COUNT(DISTINCT IF(NOT product_availability, product_id, NULL)) AS product_unavailable_ct
    , COUNT(DISTINCT IF(product_availability_duration = 'TODAY', product_id, NULL)) AS product_unavailable_today_ct
    , COUNT(DISTINCT IF(product_availability_duration = 'INDEFINITELY', product_id, NULL)) AS product_unavailable_indefinitely_ct
    , COUNT(DISTINCT IF(NOT product_availability AND event_name = 'order_product_set_availability', product_id, NULL)) AS product_unavailable_order_ct
    , COUNTIF(event_name = 'vendor_status_changed' AND vendor_status = 'UNAVAILABLE') AS vendor_unavailable_event_ct
    , COUNTIF(event_name = 'vendor_status_changed' AND vendor_status = 'UNAVAILABLE' AND vendor_unavailable_duration = 'TIME_LIMITED') AS vendor_unavailable_time_limited_event_ct
    , COUNTIF(event_name = 'vendor_status_changed' AND vendor_status = 'UNAVAILABLE' AND vendor_unavailable_duration = 'CURRENT_DAY') AS vendor_unavailable_today_event_ct
    , COUNT(DISTINCT IF(event_name = 'vendor_status_changed' AND vendor_status = 'UNAVAILABLE', uniqule_vendor_code, NULL)) AS vendor_unavailable_vendor_id_ct
    , COUNT(DISTINCT IF(event_name = 'vendor_status_changed' AND vendor_status = 'UNAVAILABLE' AND vendor_unavailable_duration = 'CURRENT_DAY', uniqule_vendor_code, NULL)) AS vendor_unavailable_today_vendor_code_ct
    , COUNTIF(event_name = 'tutorial_onboarding_test_order_button_clicked') AS test_order_sent_event_ct
    , COUNTIF(event_name = 'test_order_success_shown') AS test_order_success_event_ct
    , COUNTIF(event_name = 'test_order_failure_shown') AS test_order_fail_event_ct
    , COUNTIF(test_order_sent_at = first_test_order_sent_at) AS test_order_sent_event_1st_ct
    , COUNTIF(test_order_sent_at <> first_test_order_sent_at) AS test_order_sent_event_repeat_ct
    , COUNT(DISTINCT IF(event_name = 'tutorial_onboarding_test_order_button_clicked', uniqule_vendor_code, NULL)) AS test_order_vendor_code_ct
    , COUNTIF(event_name = 'sound_selection') AS sound_selection_event_ct
    , COUNTIF(event_name = 'sound_selection' AND sound_type = 'Default') AS sound_selection_default_event_ct
    , COUNTIF(event_name = 'sound_selection' AND sound_type = 'Custom') AS sound_selection_custom_event_ct
    , COUNTIF(event_name = 'sound_volume_changed') AS sound_volume_changed_event_ct
    , COUNTIF(event_name = 'sound_volume_changed' AND sound_volume = 'high') AS sound_volume_changed_high_event_ct
    , COUNTIF(event_name = 'sound_volume_changed' AND sound_volume = 'medium') AS sound_volume_changed_medium_event_ct
    , COUNTIF(event_name = 'sound_volume_changed' AND sound_volume = 'low') AS sound_volume_changed_low_event_ct
    , COUNTIF(event_name IN ('connectivity_warning_shown', 'idle_cover_shown')) AS overlay_event_ct
    , COUNTIF(event_name = 'idle_cover_shown') AS overlay_idle_event_ct
    , COUNTIF(event_name = 'connectivity_warning_shown') AS overlay_connectivity_event_ct
    , COUNTIF(event_name = 'connectivity_warning_shown' AND network_type = 'wifi') AS overlay_connectivity_wifi_event_ct
    , COUNTIF(event_name = 'connectivity_warning_shown' AND network_type = 'cellular') AS overlay_connectivity_cellular_event_ct
    , COUNT(DISTINCT IF(event_name = 'connectivity_warning_shown', uniqule_vendor_code, NULL)) AS overlay_connectivity_vendor_code_ct
    , COUNTIF(event_name = 'connectivity_warning_overlay_dismissed') AS overlay_connectivity_dismissed_event_ct
    , COUNTIF(event_name = 'connectivity_warning_overlay_dismissed' AND dismissed_by = 'auto') AS overlay_connectivity_dismissed_auto_event_ct
    , COUNTIF(event_name = 'connectivity_warning_overlay_dismissed' AND dismissed_by = 'user') AS overlay_connectivity_dismissed_user_event_ct
    , COUNTIF(event_name = 'idle_cover_shown' AND new_orders_count = 1) AS overlay_idle_event_1_order_ct
    , COUNTIF(event_name = 'idle_cover_shown' AND new_orders_count = 2) AS overlay_idle_event_2_order_ct
    , COUNTIF(event_name = 'idle_cover_shown' AND new_orders_count = 3) AS overlay_idle_event_3_order_ct
    , COUNTIF(event_name = 'idle_cover_shown' AND new_orders_count >= 4) AS overlay_idle_event_others_order_ct
    , COUNT(DISTINCT IF(event_name = 'tutorial_onboarding_launch', uniqule_vendor_code, NULL)) AS tutorial_onboarding_launch_vendor_code_ct
    , COUNTIF(event_name = 'tutorial_onboarding_launch') AS tutorial_onboarding_launch_event_ct
    , COUNTIF(event_name = 'tutorial_onboarding_launch' AND onboarding_from = 'INITIATION') AS tutorial_onboarding_launch_from_initiation_event_ct
    , COUNTIF(event_name = 'tutorial_onboarding_launch' AND onboarding_from = 'NAVIGATION') AS tutorial_onboarding_launch_from_navigation_event_ct
    , COUNTIF(event_name = 'tutorial_onboarding_test_order_button_clicked') AS tutorial_onboarding_test_order_button_clicked_event_ct
    , COUNTIF(event_name = 'tutorial_onboarding_start_button_clicked') AS tutorial_onboarding_start_button_clicked_event_ct
    , COUNTIF(event_name = 'login_started') AS login_started_event_ct
    , COUNTIF(event_name = 'login_failed') AS login_failed_event_ct
    , COUNTIF(event_name = 'login_failed' AND login_type = 'ALREADYLOGGEDIN') AS login_failed_already_loggedin_event_ct
    , COUNTIF(event_name = 'login_failed' AND login_type = 'UUIDLOGIN') AS login_failed_uuid_event_ct
    , COUNTIF(event_name = 'login_failed' AND login_type = 'MANUALLOGIN') AS login_failed_manual_event_ct
    , COUNTIF(event_name = 'login_failed' AND login_type = 'AUTHLOGIN') AS login_failed_auth_event_ct
    , COUNTIF(event_name = 'login_failed' AND login_type = 'TOKENLOGIN') AS login_failed_token_event_ct
  FROM rps_vendor_client_events
  GROUP BY 1,2,3,4,5,6
)
SELECT ev.*
  , CAST(ev.created_hour AS DATE) AS created_date
  , EXTRACT(HOUR FROM ev.created_hour) AS created_hour_of_day
  , o.orders_ct
  , o.fail_orders_ct
  , o.fail_orders_vendor_ct
  , o.fail_orders_product_unavailable_ct
  , o.fail_orders_no_response_ct
  , o.fail_orders_too_busy_ct
  , o.fail_orders_closed_ct
  , CASE
      WHEN EXTRACT(DAYOFWEEK FROM ev.created_hour) = 1
        THEN 'Sunday'
      WHEN EXTRACT(DAYOFWEEK FROM ev.created_hour) = 2
        THEN 'Monday'
      WHEN EXTRACT(DAYOFWEEK FROM ev.created_hour) = 3
        THEN 'Tuesday'
      WHEN EXTRACT(DAYOFWEEK FROM ev.created_hour) = 4
        THEN 'Wednesday'
      WHEN EXTRACT(DAYOFWEEK FROM ev.created_hour) = 5
        THEN 'Thursday'
      WHEN EXTRACT(DAYOFWEEK FROM ev.created_hour) = 6
        THEN 'Friday'
      WHEN EXTRACT(DAYOFWEEK FROM ev.created_hour) = 7
        THEN 'Saturday'
    END AS created_dow
  , EXTRACT(YEAR FROM ev.created_hour) AS created_year
  , FORMAT_TIMESTAMP('%Y-%m', ev.created_hour) AS created_month
  , FORMAT_TIMESTAMP('%Y-%V', ev.created_hour) AS created_week
  , en.region
  , en.brand_name
  , en.country_name
FROM rps_client_dashboard_events_agg ev
LEFT JOIN rps_orders_agg o ON ev.created_hour = o.created_hour
  AND ev.entity_id = o.entity_id
  AND ev.client_name = o.client_name
  AND ev.client_version = o.client_version
  AND ev.client_wrapper_type = o.client_wrapper_type
  AND ev.client_wrapper_version = o.client_wrapper_version
INNER JOIN entities en ON ev.entity_id = en.entity_id
