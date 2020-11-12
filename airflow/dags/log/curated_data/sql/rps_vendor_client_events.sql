WITH go_droid_events AS (
  SELECT *
    , LOWER(country_iso) AS country_code
  FROM `{{ params.project_id }}.cl._rps_godroid_data_raw`
{%- if not params.backfill %}
  WHERE created_date BETWEEN DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY) AND '{{ next_ds }}'
{%- endif %}
), gowin_events AS (
  SELECT *
    , LOWER(country_iso) AS country_code
  FROM `{{ params.project_id }}.cl._rps_gowin_data_raw`
{%- if not params.backfill %}
  WHERE created_date BETWEEN DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY) AND '{{ next_ds }}'
{%- endif %}
), rps_client_events AS (
  SELECT e.created_date
    , e.created_at
    , e.event_name
    , e._original_event_name
    , e.event_scope
    , e.experiments
    , e._operator_code
    , e.vendor_id
    , e.vendor_code
    , e.platform_key
    , e.entity_id
    , e.country_code
    , e.country_iso
    , e.timezone
    , e.region
    , STRUCT(
        e.order_id AS id
        , e.order_code AS code
        , e.delivery_type
        , CAST(e.is_preorder AS BOOL) AS is_preorder
        , COALESCE(STARTS_WITH(e.order_code, 'TEST') OR STARTS_WITH(e.order_code, 'TOTO'), FALSE) AS is_test_order
        , e.order_status AS state
        , e.cancelled_reason AS cancelled_reason
        , e.order_minutes_suggested AS suggested_minutes
        , e.order_minutes AS minutes
        , CAST(NULL AS STRING) AS payment_method
      ) AS orders
    , STRUCT(
        e.product_id AS id
        , e.product_name  AS name
        , e.product_availability AS availability
        , e.product_availability_duration AS availability_duration
      ) AS product
    , STRUCT(
        'GODROID' AS name
        , e.client_version AS version
        , CAST(NULL AS STRING) AS wrapper_type
        , CAST(NULL AS STRING) AS wrapper_version
      ) AS client
    , STRUCT(
        e.device_id AS id
        , e.device_os AS os_name
        , e.device_os_version AS os_version
        , e.device_brand AS brand
        , e.device_model AS model
      ) AS device
    , STRUCT(
        e.connectivity_network_type AS network_type
        , e.connectivity_dismissed_by AS dismissed_by
      ) AS connectivity
    , STRUCT(
        e.vendor_status AS status
        , e.vendor_unavailable_duration AS unavailable_duration
        , e.vendor_minutes AS minutes
      ) AS vendor
    , STRUCT(
        e.printer_connection_duration AS connection_duration
        , e.printer_name AS name
        , e.printer_type AS type
        , CAST(NULL AS STRING) AS printing_event_type
        , STRUCT (
            e.printer_receipts_number AS receipts_number
            , e.printer_printing_duration AS printing_duration
          ) AS print_receipt
      ) AS printer
    , STRUCT(
        e.menu_search_query AS search_query
        , e.menu_search_type AS search_type
        , e.menu_search_results_count AS search_results_count
        , e.menu_is_products_shown AS is_products_shown
        , e.menu_is_products_selected AS is_products_selected
      ) AS menu
    , STRUCT(
        CAST(NULL AS STRING) AS type
        , e.login_selected_country_iso AS selected_country_iso
      ) AS login
    , STRUCT(
        e.checkin_availability_status AS availability_status
        , e.checkin_next_opening_at AS next_opening_at
      ) AS checkin
    , STRUCT(
        e.auxiliary_new_orders_count AS new_orders_count
        , e.auxiliary_previous_locale AS previous_locale
        , e.auxiliary_new_locale AS new_locale
        , e.auxiliary_tutorial_name AS tutorial_name
        , e.auxiliary_onboarding_from AS onboarding_from
        , e.auxiliary_onboarding_page AS onboarding_page
        , e.auxiliary_onboarding_action AS onboarding_action
        , e.auxiliary_context AS onboarding_context
        , e.auxiliary_ringtone AS sound_type
        , e.auxiliary_volume AS sound_volume
        , e.auxiliary_navigation_menu_item AS navigation_menu_item
        , e.auxiliary_notification_displayed AS notification_displayed
        , e.auxiliary_device_restarted_reason AS device_restarted_reason
      ) AS auxiliary
  FROM go_droid_events e
  UNION ALL
  SELECT e.created_date
    , e.created_at
    , e.event_name
    , e._original_event_name
    , e.event_scope
    , CAST(NULL AS STRING) AS experiments
    , e._operator_code
    , e.vendor_id
    , e.vendor_code
    , e.platform_key
    , e.entity_id
    , IF(e.country_code <> 'na', e.country_code, NULL) AS country_code
    , e.country_iso
    , e.timezone
    , e.region
    , STRUCT(
        e.order_id AS id
        , e.order_code AS code
        , e.delivery_type
        , e.is_preorder
        , COALESCE(STARTS_WITH(e.order_code, 'TEST') OR STARTS_WITH(e.order_code, 'TOTO'), FALSE) AS is_test_order
        , e.order_status AS state
        , e.order_cancelled_reason AS cancelled_reason
        , NULL AS suggested_minutes
        , e.order_minutes AS minutes
        , e.order_payment_method AS payment_method
      ) AS orders
    , STRUCT(
        e.product_id AS id
        , e.product_name  AS name
        , e.product_availability AS availability
        , e.product_availability_duration AS availability_duration
      ) AS product
    , STRUCT(
        'GOWIN' AS name
        , e.client_version AS version
        , e.client_wrapper AS wrapper_type
        , e.client_wrapper_version AS wrapper_version
      ) AS client
    , STRUCT(
        e.device_id AS id
        , e.device_os AS os_name
        , e.device_os_version AS os_version
        , e.device_brand AS brand
        , e.device_model AS model
      ) AS device
    , STRUCT(
        CAST(NULL AS STRING) AS network_type
        , e.connectivity_dismissed_by AS dismissed_by
      ) AS connectivity
    , STRUCT(
        e.vendor_status AS status
        , e.vendor_unavailable_duration AS unavailable_duration
        , IF(e.vendor_unavailable_duration = 'TIME_LIMITED', e.vendor_minutes, NULL) AS minutes
      ) AS vendor
    , STRUCT(
        CAST(NULL AS FLOAT64) AS connection_duration
        , CAST(NULL AS STRING) AS name
        , CAST(NULL AS STRING) AS type
        , printer_printing_event_type AS printing_event_type
        , STRUCT(
            CAST(NULL AS INT64) AS receipts_number
            , CAST(NULL AS FLOAT64) AS printing_duration
          ) AS print_receipt
      ) AS printer
    , STRUCT(
        CAST(NULL AS STRING) AS search_query
        , CAST(NULL AS STRING) AS search_type
        , CAST(NULL AS INT64) AS search_results_count
        , CAST(NULL AS BOOL) AS is_product_ids_shown
        , CAST(NULL AS BOOL) AS is_product_ids_selected
      ) AS menu
    , STRUCT(
        e.login_type AS type
        , CAST(NULL AS STRING) AS selected_country_iso
      ) AS login
    , STRUCT(
        CAST(NULL AS STRING) AS availability_status
        , CAST(NULL AS TIMESTAMP) AS next_opening_at
      ) AS checkin
    , STRUCT(
        CAST(NULL AS INT64) AS new_orders_count
        , CAST(NULL AS STRING) AS previous_locale
        , CAST(NULL AS STRING) AS new_locale
        , CAST(NULL AS STRING) AS tutorial_name
        , CAST(NULL AS STRING) AS onboarding_from
        , CAST(NULL AS INT64) AS onboarding_page
        , CAST(NULL AS STRING) AS onboarding_action
        , CAST(NULL AS STRING) AS onboarding_context
        , e.auxiliary_sound_type AS sound_type
        , CAST(NULL AS STRING) AS sound_volume
        , CAST(NULL AS STRING) AS navigation_menu_item
        , CAST(NULL AS BOOL) AS notification_displayed
        , CAST(NULL AS STRING) AS device_restarted_reason
      ) AS auxiliary
  FROM gowin_events e
)
SELECT * EXCEPT (entity_id, vendor, orders, product, connectivity, printer, menu, login, checkin, auxiliary)
  , CASE
      WHEN entity_id = 'DO_RS'
        THEN 'DN_RS'
      WHEN entity_id IN ('TB_EG', 'TB_OT')
        THEN 'DN_RS'
      WHEN entity_id = 'PY_CO'
        THEN 'CD_CO'
      ELSE entity_id
    END AS entity_id
  , IF(event_scope = 'vendor', vendor, NULL) AS vendor
  , IF(event_scope IN ('order', 'product'), orders, NULL) AS orders
  , IF(event_scope = 'product' OR event_name IN ('order_product_set_availability', 'order_decline_set_item_unavailable', 'order_product_set_availability', 'order_reconcile_accept', 'order_reconcile_decline'), product, NULL) AS product
  , IF(event_scope = 'printer', printer, NULL) AS printer
  , IF(event_scope = 'menu', menu, NULL) AS menu
  , IF(event_scope = 'connectivity', connectivity, NULL) AS connectivity
  , IF(event_scope = 'login', login, NULL) AS login
  , IF(event_scope = 'checkin', checkin, NULL) AS checkin
  , IF(event_scope = 'auxiliary', auxiliary, NULL) AS auxiliary
FROM rps_client_events
