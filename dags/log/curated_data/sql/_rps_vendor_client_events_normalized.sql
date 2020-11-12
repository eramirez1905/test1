CREATE OR REPLACE table `{{ params.project_id }}.cl._rps_vendor_client_events_normalized` AS
  SELECT 'order_recevied' AS event_name, 'receive_order_new' AS godroid_event_name_raw, 'order.received' AS gowin_event_name_raw, 'order' AS event_scope, 'RECEIVED_BY_VENDOR' AS order_status, TRUE AS is_platform_specific, FALSE AS is_fail_order_status
  UNION ALL
  SELECT 'order_processed', 'receive_order_processed', NULL, 'order', 'ACCEPTED_BY_VENDOR', TRUE, FALSE
  UNION ALL
  SELECT 'order_upcoming', 'receive_order_upcoming', NULL, 'order', 'WAITING_FOR_TRANSPORT', TRUE, FALSE
  UNION ALL
  SELECT 'order_accepted', 'user_order_accept', 'order.accepted', 'order', 'ACCEPTED_BY_VENDOR', TRUE, FALSE
  UNION ALL
  SELECT 'order_dispatched', 'user_order_dispatch', 'order.dispatched', 'order', 'WAITING_FOR_TRANSPORT', TRUE, FALSE
  UNION ALL
  SELECT 'order_dispatched_early_warning_shown', 'early_dispatch_warning_shown', 'order.dispatched_early_warning_shown', 'order', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'order_dispatch_reminder_shown', 'dispatch_reminder_shown', 'order.dispatch_reminder_shown', 'order', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'order_dispatch_reminder_clicked', 'user_dispatch_reminder_clicked', NULL, 'order', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'order_pickedup', 'user_order_pickup', 'order.pickedup', 'order', 'PICKED_UP', TRUE, FALSE
  UNION ALL
  SELECT 'order_not_pickedup', 'user_order_not_picked_up', NULL, 'order', 'CANCELLED', TRUE, FALSE
  UNION ALL
  SELECT 'order_rejected', 'user_order_decline', 'order.rejected', 'order', 'CANCELLED', TRUE, TRUE
  UNION ALL
  SELECT 'order_product_set_availability', 'ack_set_product_availability', NULL, 'order', 'CANCELLED', TRUE, TRUE
  UNION ALL
  SELECT 'order_cancelled', NULL, 'order.cancelled', 'order', 'CANCELLED', TRUE, TRUE
  UNION ALL
  SELECT 'order_expired', 'order_expired', 'order.expired', 'order', 'CANCELLED', TRUE, TRUE
  UNION ALL
  SELECT 'order_cancelled_by_external', 'receive_order_cancel', 'order.external_cancellation_acknowledged', 'order', 'CANCELLED', TRUE, TRUE
  UNION ALL
  SELECT 'order_delayed_by_rider', 'receive_rider_delay', NULL, 'order', 'DELAYED', TRUE, FALSE
  UNION ALL
  SELECT 'order_delayed', NULL, 'order.delayed', 'order', 'DELAYED', TRUE, FALSE
  UNION ALL
  SELECT 'vendor_order_list_sorted', NULL, 'order_list.sorted', 'vendor', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'vendor_status_changed', 'user_vendor_set_available', NULL, 'vendor', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'vendor_status_changed', 'user_vendor_set_busy', NULL, 'vendor', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'vendor_status_changed', 'user_vendor_set_unavailable', NULL, 'vendor', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'vendor_status_changed', 'user_set_vendor_availability', 'vendor_status_changed', 'vendor', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'product_set_availability', 'user_product_enable', 'product_set_availability', 'product', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'product_set_availability', 'user_product_disable', 'product_set_availability', 'product', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'product_set_availability', 'user_product_disable_indefinitely', 'product_set_availability', 'product', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'printer_selected', 'printer_selected', NULL, 'printer', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'printer_connected', 'printer_connected', NULL, 'printer', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'printer_print_receipt', 'printer_print_receipt', 'printer_print_receipt', 'printer', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'printer_print_receipt_copy', 'printer_print_receipt_copy', NULL, 'printer', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'printer_print_test', 'printer_print_test', NULL, 'printer', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'connectivity_warning_shown', 'offline_overlay_shown', 'connectivity_warning_shown', 'connectivity', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'connectivity_warning_dismissed', NULL, 'connectivity_warning_dismissed', 'connectivity', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'connectivity_warning_overlay_dismissed', 'offline_overlay_dismissed', NULL, 'connectivity', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'login_started', NULL, 'login_started', 'login', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'login_started', NULL, 'login.started', 'login', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'login_country_selected', 'discovery_country_selected', NULL, 'login', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'login_succeeded', NULL, 'login.succeeded', 'login', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'login_failed', NULL, 'login.failed', 'login', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'logout_submitted', NULL, 'logout.submitted', 'login', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'alarm_started', NULL, 'alarm.started', 'alarm', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'alarm_muted', NULL, 'alarm.muted', 'alarm', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'idle_cover_shown', 'show_idle_cover', NULL, 'auxiliary', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'order_rider_info_clicked', 'user_rider_info_clicked', NULL, 'order', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'order_customer_info_clicked', 'user_customer_info_clicked', NULL, 'order', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'product_search_availability_updated', 'user_search_product_availability_updated', NULL, 'product', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'tutorial_shown', 'tutorial_shown', NULL, 'auxiliary', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'menu_search_executed', 'user_menu_search_executed', 'menu_search_executed', 'menu', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'menu_search_button_clicked', 'user_menu_search_button_clicked', NULL, 'menu', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'order_food_is_ready_clicked', 'user_food_is_ready_clicked', NULL, 'order', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'order_change_time_button_clicked', 'user_change_time_btn_clicked', NULL, 'order', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'order_early_dispatch_warning_dismissed', 'user_early_dispatch_warning_dismissed', 'order.dispatched_early_warning_cancelled', 'order', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'order_early_dispatch_warning_dispatched', 'user_early_dispatch_warning_dispatched', 'order.dispatched_early_warning_confirmed', 'order', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'order_decline_dialog_dismissed', 'user_order_decline_dialog_dismissed', NULL, 'order', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'google_play_service_version_changed', 'play_service_version_changed', NULL, 'auxiliary', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'order_decline_customer_dialog_shown', 'order_decline_customer_dialog_shown', NULL, 'order', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'order_decline_customer_dialog_dismissed', 'order_decline_customer_dialog_dismissed', NULL, 'order', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'order_contact_customer_accept', 'user_contact_customer_accept', NULL, 'order', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'order_contact_customer_decline', 'user_contact_customer_decline', NULL, 'order', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'order_contact_customer_clicked', 'user_contact_customer_clicked', NULL, 'order', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'order_product_set_availability', 'order_decline_set_item_unavailable', NULL, 'order', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'vendor_order_history_list_search', NULL, 'order_history_list_search', 'vendor', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'locale_changed', 'user_locale_changed', NULL, 'auxiliary', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'tutorial_onboarding_launch', 'onboarding_launch', NULL, 'auxiliary', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'tutorial_onboarding_next_page', 'onboarding_next_page', NULL, 'auxiliary', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'tutorial_onboarding_start_button_clicked', 'onboarding_start_button_clicked', NULL, 'auxiliary', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'application_crash', 'app_exception', NULL, 'auxiliary', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'order_reconcile_accept', 'user_reconcile_accept', NULL, 'order', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'order_reconcile_decline', 'user_reconcile_decline', NULL, 'order', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'sound_selection', 'sound_ringtone_changed', 'sound_selection', 'auxiliary', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'navigation_drawer_shown', 'nav_drawer_shown', NULL, 'auxiliary', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'navigation_drawer_item_clicked', 'nav_drawer_item_clicked', NULL, 'auxiliary', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'sound_tested', 'test_sound_clicked', NULL, 'auxiliary', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'sound_volume_changed', 'sound_volume_changed', NULL, 'auxiliary', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'tutorial_onboarding_test_order_button_clicked', 'onboarding_test_order_button_clicked', NULL, 'auxiliary', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'tutorial_onboarding_closed', 'onboarding_close_button', NULL, 'auxiliary', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'test_order_success_shown', 'test_order_success_shown', NULL, 'auxiliary', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'test_order_failure_shown', 'test_order_failure_shown', NULL, 'auxiliary', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'checkin_displayed', 'checkin_displayed', NULL, 'checkin', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'checkin_clicked', 'checkin_clicked', NULL, 'checkin', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'checkin_error', 'checkin_error', NULL, 'checkin', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'device_restarted', 'device_restarted', NULL, 'auxiliary', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'order_quick_acceptance_screen_neutral_shown', 'quick_acceptance_screen_neutral_shown', NULL, 'order', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'order_quick_acceptance_screen_positive_shown', 'quick_acceptance_screen_positive_shown', NULL, 'order', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'self_activation_component_shown', NULL, 'self_activation_component_shown', 'self_activation', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'self_activation_component_activate', NULL, 'user_self_activation_component_activate', 'self_activation', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'self_activation_component_tutorial', NULL, 'user_self_activation_component_tutorial', 'self_activation', NULL, FALSE, FALSE
  UNION ALL
  SELECT 'menu_item_alert_shown', 'menu_item_alert_shown', NULL, 'menu', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'menu_item_alert_clicked', 'menu_item_alert_clicked', NULL, 'menu', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'menu_item_component_shown', 'menu_item_component_shown', NULL, 'menu', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'menu_item_component_dismissed', 'menu_item_component_dismissed', NULL, 'menu', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'menu_item_component_updated', 'menu_item_component_updated', NULL, 'menu', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'menu_item_component_unchanged', 'menu_item_component_unchanged', NULL, 'menu', NULL, TRUE, FALSE
  UNION ALL
  SELECT 'menu_item_component_error', 'menu_item_component_error', NULL, 'menu', NULL, TRUE, FALSE
ORDER BY event_scope, event_name
