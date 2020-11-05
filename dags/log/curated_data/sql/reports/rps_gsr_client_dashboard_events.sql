CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.rps_gsr_client_dashboard_events`
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
), rps_gsr_client_dashboard_events_hourly_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.rl.rps_gsr_client_dashboard_events_hourly`
  WHERE created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 3 MONTH)
), rps_gsr_client_dashboard_events_hourly AS (
  SELECT created_date
    , entity_id
    , client_name
    , client_version
    , client_wrapper_type
    , client_wrapper_version
    , SUM(vendor_id_ct) AS vendor_id_ct
    , SUM(vendor_code_ct) AS vendor_code_ct
    , SUM(orders_ct) AS orders_ct
    , SUM(fail_orders_ct) AS fail_orders_ct
    , SUM(fail_orders_vendor_ct) AS fail_orders_vendor_ct
    , SUM(fail_orders_product_unavailable_ct) AS fail_orders_product_unavailable_ct
    , SUM(fail_orders_no_response_ct) AS fail_orders_no_response_ct
    , SUM(fail_orders_too_busy_ct) AS fail_orders_too_busy_ct
    , SUM(fail_orders_closed_ct) AS fail_orders_closed_ct
    , SUM(product_unavailable_ct) AS product_unavailable_ct
    , SUM(product_unavailable_today_ct) AS product_unavailable_today_ct
    , SUM(product_unavailable_indefinitely_ct) AS product_unavailable_indefinitely_ct
    , SUM(product_unavailable_order_ct) AS product_unavailable_order_ct
    , SUM(vendor_unavailable_event_ct) AS vendor_unavailable_event_ct
    , SUM(vendor_unavailable_time_limited_event_ct) AS vendor_unavailable_time_limited_event_ct
    , SUM(vendor_unavailable_today_event_ct) AS vendor_unavailable_today_event_ct
    , SUM(vendor_unavailable_vendor_id_ct) AS vendor_unavailable_vendor_id_ct
    , SUM(vendor_unavailable_today_vendor_code_ct) AS vendor_unavailable_today_vendor_code_ct
    , SUM(test_order_sent_event_ct) AS test_order_sent_event_ct
    , SUM(test_order_success_event_ct) AS test_order_success_event_ct
    , SUM(test_order_fail_event_ct) AS test_order_fail_event_ct
    , SUM(test_order_sent_event_1st_ct) AS test_order_sent_event_1st_ct
    , SUM(test_order_sent_event_repeat_ct) AS test_order_sent_event_repeat_ct
    , SUM(test_order_vendor_code_ct) AS test_order_vendor_code_ct
    , SUM(sound_selection_event_ct) AS sound_selection_event_ct
    , SUM(sound_selection_default_event_ct) AS sound_selection_default_event_ct
    , SUM(sound_selection_custom_event_ct) AS sound_selection_custom_event_ct
    , SUM(sound_volume_changed_event_ct) AS sound_volume_changed_event_ct
    , SUM(sound_volume_changed_high_event_ct) AS sound_volume_changed_high_event_ct
    , SUM(sound_volume_changed_medium_event_ct) AS sound_volume_changed_medium_event_ct
    , SUM(sound_volume_changed_low_event_ct) AS sound_volume_changed_low_event_ct
    , SUM(overlay_event_ct) AS overlay_event_ct
    , SUM(overlay_idle_event_ct) AS overlay_idle_event_ct
    , SUM(overlay_connectivity_event_ct) AS overlay_connectivity_event_ct
    , SUM(overlay_connectivity_wifi_event_ct) AS overlay_connectivity_wifi_event_ct
    , SUM(overlay_connectivity_cellular_event_ct) AS overlay_connectivity_cellular_event_ct
    , SUM(overlay_connectivity_vendor_code_ct) AS overlay_connectivity_vendor_code_ct
    , SUM(overlay_connectivity_dismissed_event_ct) AS overlay_connectivity_dismissed_event_ct
    , SUM(overlay_connectivity_dismissed_auto_event_ct) AS overlay_connectivity_dismissed_auto_event_ct
    , SUM(overlay_connectivity_dismissed_user_event_ct) AS overlay_connectivity_dismissed_user_event_ct
    , SUM(overlay_idle_event_1_order_ct) AS overlay_idle_event_1_order_ct
    , SUM(overlay_idle_event_2_order_ct) AS overlay_idle_event_2_order_ct
    , SUM(overlay_idle_event_3_order_ct) AS overlay_idle_event_3_order_ct
    , SUM(overlay_idle_event_others_order_ct) AS overlay_idle_event_others_order_ct
    , SUM(tutorial_onboarding_launch_vendor_code_ct) AS tutorial_onboarding_launch_vendor_code_ct
    , SUM(tutorial_onboarding_launch_event_ct) AS tutorial_onboarding_launch_event_ct
    , SUM(tutorial_onboarding_launch_from_initiation_event_ct) AS tutorial_onboarding_launch_from_initiation_event_ct
    , SUM(tutorial_onboarding_launch_from_navigation_event_ct) AS tutorial_onboarding_launch_from_navigation_event_ct
    , SUM(tutorial_onboarding_test_order_button_clicked_event_ct) AS tutorial_onboarding_test_order_button_clicked_event_ct
    , SUM(tutorial_onboarding_start_button_clicked_event_ct) AS tutorial_onboarding_start_button_clicked_event_ct
    , SUM(login_started_event_ct) AS login_started_event_ct
    , SUM(login_failed_event_ct) AS login_failed_event_ct
    , SUM(login_failed_already_loggedin_event_ct) AS login_failed_already_loggedin_event_ct
    , SUM(login_failed_uuid_event_ct) AS login_failed_uuid_event_ct
    , SUM(login_failed_manual_event_ct) AS login_failed_manual_event_ct
    , SUM(login_failed_auth_event_ct) AS login_failed_auth_event_ct
    , SUM(login_failed_token_event_ct) AS login_failed_token_event_ct
  FROM rps_gsr_client_dashboard_events_hourly_dataset
  GROUP BY 1,2,3,4,5,6
)
SELECT ev.*
  , CASE
      WHEN EXTRACT(DAYOFWEEK FROM ev.created_date) = 1
        THEN 'Sunday'
      WHEN EXTRACT(DAYOFWEEK FROM ev.created_date) = 2
        THEN 'Monday'
      WHEN EXTRACT(DAYOFWEEK FROM ev.created_date) = 3
        THEN 'Tuesday'
      WHEN EXTRACT(DAYOFWEEK FROM ev.created_date) = 4
        THEN 'Wednesday'
      WHEN EXTRACT(DAYOFWEEK FROM ev.created_date) = 5
        THEN 'Thursday'
      WHEN EXTRACT(DAYOFWEEK FROM ev.created_date) = 6
        THEN 'Friday'
      WHEN EXTRACT(DAYOFWEEK FROM ev.created_date) = 7
        THEN 'Saturday'
    END AS created_dow
  , EXTRACT(YEAR FROM ev.created_date) AS created_year
  , FORMAT_DATE('%Y-%m', ev.created_date) AS created_month
  , FORMAT_DATE('%Y-%V', ev.created_date) AS created_week
  , en.region
  , en.brand_name
  , en.country_name
FROM rps_gsr_client_dashboard_events_hourly ev
INNER JOIN entities en ON ev.entity_id = en.entity_id
