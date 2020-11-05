CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.rps_gsr_client_dashboard_order_events`
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
), rps_orders_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl.rps_orders`
  WHERE created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 3 MONTH)
), rps_orders AS (
  SELECT region
    , order_id
    , order_status = 'cancelled' AS is_fail
    , cancellation.reason AS cancellation_reason
    , cancellation.owner AS cancellation_owner
  FROM rps_orders_dataset
), rps_vendor_client_events_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl.rps_vendor_client_events`
  WHERE created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 3 MONTH)
), events AS (
  SELECT ev.region
    , ev.vendor_id
    , MIN(DATE(ev.created_at, ev.timezone)) AS first_test_order_date
  FROM rps_vendor_client_events_dataset ev
  WHERE event_name = 'tutorial_onboarding_test_order_button_clicked'
  GROUP BY 1,2
), order_events AS (
  SELECT DATE(ev.created_at, ev.timezone) AS created_date
    , ev.region
    , ev.entity_id
    , ev.country_code
    , ev.client.name AS client_name
    , ev.client.version AS client_version
    , COALESCE(ev.client.wrapper_type, '(N/A)') AS wrapper_type
    , COALESCE(ev.client.wrapper_version, '(N/A)') AS wrapper_version
    , ev.device.os_name
    , ev.device.os_version
    , ev.device.model AS device_model
    , ev.device.brand AS device_brand
    , ev.vendor_id
    , ev.vendor_code
    , CONCAT(ev.region , '-', ev.vendor_id ) AS uniqule_vendor_id
    , CONCAT(ev.entity_id , '-', ev.vendor_code ) AS uniqule_vendor_code
    , ev.device.id AS device_id
    , ev.orders.id AS order_id
    , COALESCE(ev.orders.delivery_type, '(UNKONWN)') AS delivery_type
    , orders.is_preorder
    , ARRAY_AGG(
        STRUCT(
          ev.created_at
          , ev.event_name
          , ev.orders.minutes AS order_minutes
          , ev.product.id AS product_id
        )
      ) AS events
    , MAX(ev.created_at) AS created_at
  FROM rps_vendor_client_events_dataset ev
  WHERE ev.event_scope = 'order'
    AND NOT ev.orders.is_test_order
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
), order_events_clean AS (
  SELECT *
  FROM (
    SELECT *
      , ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY created_at DESC) AS _row_number
    FROM order_events
  ) WHERE _row_number = 1
), order_with_timestamps AS (
  SELECT *
    , TIMESTAMP_DIFF(order_accepted_at, order_recevied_at, SECOND) AS response_time_client
    , TIMESTAMP_DIFF(order_dispatched_at, order_accepted_at, SECOND) AS dispatch_time
  FROM (
    SELECT created_date
      , region
      , entity_id
      , order_id
      , delivery_type
      , is_preorder
      , (SELECT order_minutes FROM UNNEST(events) WHERE event_name = 'order_accepted' AND order_minutes IS NOT NULL ORDER BY created_at LIMIT 1) AS order_promised_for
      , (SELECT MIN(created_at) FROM UNNEST(events) WHERE event_name = 'order_recevied') AS order_recevied_at
      , (SELECT MIN(created_at) FROM UNNEST(events) WHERE event_name = 'order_accepted') AS order_accepted_at
      , (SELECT MIN(created_at) FROM UNNEST(events) WHERE event_name = 'order_food_is_ready_clicked') AS order_dispatched_at
      , (SELECT COUNT(*) FROM UNNEST(events) WHERE event_name IN ('order_reconcile_accept', 'order_reconcile_decline', 'order_decline_customer_dialog_dismissed', 'order_decline_customer_dialog_shown', 'order_decline_dialog_dismissed')) > 0 AS is_reconciled
      , (SELECT COUNT(*) FROM UNNEST(events) WHERE event_name IN ('order_reconcile_accept', 'order_contact_customer_accept')) > 0 AS is_reconciled_and_accepted
      , (SELECT COUNT(*) FROM UNNEST(events) WHERE event_name IN ('order_reconcile_decline', 'order_contact_customer_decline')) > 0 AS is_reconciled_and_declined
      , (SELECT COUNT(*) FROM UNNEST(events) WHERE event_name IN ('order_reconcile_accept', 'order_reconcile_decline')) > 0 AS is_reconciled_partial_acceptance
      , (SELECT COUNT(*) FROM UNNEST(events) WHERE event_name = 'order_reconcile_accept') > 0 AS is_reconciled_and_accepted_partial_acceptance
      , (SELECT COUNT(*) FROM UNNEST(events) WHERE event_name = 'order_reconcile_decline') > 0 AS is_reconciled_and_declined_partial_acceptance
      , (SELECT COUNT(*) FROM UNNEST(events) WHERE event_name IN ('order_contact_customer_accept', 'order_contact_customer_decline')) > 0 AS is_reconciled_customer_dialog
      , (SELECT COUNT(*) FROM UNNEST(events) WHERE event_name = 'order_contact_customer_accept') > 0 AS is_reconciled_and_accepted_customer_dialog
      , (SELECT COUNT(*) FROM UNNEST(events) WHERE event_name = 'order_contact_customer_decline') > 0 AS is_reconciled_and_declined_customer_dialog
      , (SELECT COUNT(product_id) FROM UNNEST(events) WHERE event_name IN ('order_reconcile_accept', 'order_reconcile_decline')) AS reconciled_product_ct
      , (SELECT COUNT(product_id) FROM UNNEST(events) WHERE event_name = 'order_reconcile_accept') AS reconciled_and_accepted_product_ct
      , (SELECT COUNT(product_id) FROM UNNEST(events) WHERE event_name = 'order_reconcile_decline') AS reconciled_and_declined_product_ct
      , (SELECT COUNT(*) FROM UNNEST(events) WHERE event_name IN ('order_customer_info_clicked', 'order_rider_info_clicked')) > 0 AS is_info_checked
      , (SELECT COUNT(*) FROM UNNEST(events) WHERE event_name = 'order_customer_info_clicked') > 0 AS is_info_customer_checked
      , (SELECT COUNT(*) FROM UNNEST(events) WHERE event_name = 'order_rider_info_clicked') > 0 AS is_info_rider_checked
    FROM order_events_clean
  )
), rps_vendor_client_order_events AS (
  SELECT ev.created_date
    , EXTRACT(DAYOFWEEK FROM ev.created_date) AS created_dow
    , ev.region
    , ev.entity_id
    , ev.client_name
    , ev.client_version
    , ev.wrapper_type
    , ev.wrapper_version
    , ev.os_name
    , ev.os_version
    , ev.device_model
    , ev.device_brand
    , ev.vendor_id
    , CONCAT(ev.region , '-', ev.vendor_id ) AS uniqule_vendor_id
    , CONCAT(ev.entity_id, '-', ev.vendor_code ) AS uniqule_vendor_code
    , ev.device_id
    , ev.order_id
    , ev.delivery_type
    , ev.is_preorder
    , ot.order_promised_for
    , ot.response_time_client
    , ot.dispatch_time
    , ot.is_reconciled
    , ot.is_reconciled_and_accepted
    , ot.is_reconciled_and_declined
    , ot.is_reconciled_partial_acceptance
    , ot.is_reconciled_and_accepted_partial_acceptance
    , ot.is_reconciled_customer_dialog
    , ot.is_reconciled_and_accepted_customer_dialog
    , ot.is_reconciled_and_declined_customer_dialog
    , ot.reconciled_product_ct
    , ot.reconciled_and_accepted_product_ct
    , ot.reconciled_and_declined_product_ct
    , ot.is_info_checked
    , ot.is_info_customer_checked
    , ot.is_info_rider_checked
    , ROW_NUMBER() OVER (PARTITION BY ev.order_id ORDER BY created_at) AS _row_number_per_order
  FROM order_events_clean ev
  LEFT JOIN order_with_timestamps ot ON ev.order_id = ot.order_id
), rps_client_orders_agg AS (
  SELECT oev.created_date
    , ev.first_test_order_date
    , oev.entity_id
    , oev.client_name
    , oev.client_version
    , oev.wrapper_type
    , oev.wrapper_version
    , oev.os_name
    , oev.os_version
    , oev.device_model
    , oev.device_brand
    , oev.delivery_type
    , oev.is_preorder
    , oev.order_promised_for
    , IF(oev.response_time_client BETWEEN 0 AND 3600, oev.response_time_client, NULL) AS response_time_client_bin
    , IF(oev.dispatch_time BETWEEN 0 AND 3600, oev.dispatch_time, NULL) AS dispatch_time_bin
    , COUNT(DISTINCT oev.uniqule_vendor_id) AS vendor_id_ct
    , COUNT(DISTINCT IF(FORMAT_DATE('%Y-%V', oev.created_date) = FORMAT_DATE('%Y-%V', ev.first_test_order_date), oev.uniqule_vendor_id, NULL)) AS vendor_id_ct_tutorial
    , COUNT(DISTINCT oev.uniqule_vendor_code) AS vendor_code_ct
    , COUNT(DISTINCT oev.device_id) AS device_ct
    , COUNT(DISTINCT oev.order_id) AS order_ct
    , COUNT(DISTINCT IF(oev.is_reconciled, oev.order_id, NULL)) AS reconciled_order_ct
    , COUNT(DISTINCT IF(oev.is_reconciled_and_accepted, oev.order_id, NULL)) AS reconciled_accepted_order_ct
    , COUNT(DISTINCT IF(oev.is_reconciled_and_declined, oev.order_id, NULL)) AS reconciled_declined_order_ct
    , COUNT(DISTINCT IF(oev.is_reconciled_partial_acceptance, oev.order_id, NULL)) AS reconciled_order_partial_acceptance_ct
    , COUNT(DISTINCT IF(oev.is_reconciled_and_accepted_partial_acceptance, oev.order_id, NULL)) AS reconciled_accepted_order_partial_acceptance_ct
    , COUNT(DISTINCT IF(oev.is_reconciled_and_accepted_partial_acceptance, oev.order_id, NULL)) AS reconciled_declined_order_partial_acceptance_ct
    , COUNT(DISTINCT IF(oev.is_reconciled_customer_dialog, oev.order_id, NULL)) AS reconciled_order_customer_dialog_ct
    , COUNT(DISTINCT IF(oev.is_reconciled_and_accepted_customer_dialog, oev.order_id, NULL)) AS reconciled_accepted_order_customer_dialog_ct
    , COUNT(DISTINCT IF(oev.is_reconciled_and_declined_customer_dialog, oev.order_id, NULL)) AS reconciled_declined_order_customer_dialog_ct
    , COUNT(DISTINCT IF(oev.is_reconciled AND NOT o.is_fail, oev.order_id, NULL)) AS reconciled_successful_order_ct
    , COUNT(DISTINCT IF(oev.is_info_checked, oev.order_id, NULL)) AS info_checked_order_ct
    , COUNT(DISTINCT IF(oev.is_info_customer_checked, oev.order_id, NULL)) AS info_checked_customer_order_ct
    , COUNT(DISTINCT IF(oev.is_info_rider_checked, oev.order_id, NULL)) AS info_checked_rider_order_ct
    , COUNT(DISTINCT IF(o.is_fail, oev.order_id, NULL)) AS fail_order_ct
    , COUNT(DISTINCT IF(o.is_fail AND o.cancellation_owner = 'VENDOR', oev.order_id, NULL)) AS fail_order_vendor_ct
    , COUNT(DISTINCT IF(o.is_fail AND o.cancellation_reason = 'ITEM_UNAVAILABLE', oev.order_id, NULL)) AS fail_order_product_unavailable_ct
    , COUNT(DISTINCT IF(o.is_fail AND o.cancellation_reason = 'NO_RESPONSE', oev.order_id, NULL)) AS fail_order_no_response_ct
    , COUNT(DISTINCT IF(o.is_fail AND o.cancellation_reason = 'TOO_BUSY', oev.order_id, NULL)) AS fail_order_too_busy_ct
    , COUNT(DISTINCT IF(o.is_fail AND o.cancellation_reason = 'CLOSED', oev.order_id, NULL)) AS fail_order_closed_ct
  FROM rps_vendor_client_order_events oev
  LEFT JOIN rps_orders o ON oev.order_id = o.order_id
    AND oev.region = o.region
    AND oev._row_number_per_order = 1
  LEFT JOIN events ev ON oev.vendor_id = ev.vendor_id
    AND oev.region = ev.region
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
)
SELECT o.*
  , EXTRACT(YEAR FROM o.created_date) AS created_year
  , FORMAT_DATE('%Y-%m', o.created_date) AS created_month
  , FORMAT_DATE('%Y-%V', o.created_date) AS created_week
  , CASE
      WHEN EXTRACT(DAYOFWEEK FROM o.created_date) = 1
        THEN 'Sunday'
      WHEN EXTRACT(DAYOFWEEK FROM o.created_date) = 2
        THEN 'Monday'
      WHEN EXTRACT(DAYOFWEEK FROM o.created_date) = 3
        THEN 'Tuesday'
      WHEN EXTRACT(DAYOFWEEK FROM o.created_date) = 4
        THEN 'Wednesday'
      WHEN EXTRACT(DAYOFWEEK FROM o.created_date) = 5
        THEN 'Thursday'
      WHEN EXTRACT(DAYOFWEEK FROM o.created_date) = 6
        THEN 'Friday'
      WHEN EXTRACT(DAYOFWEEK FROM o.created_date) = 7
        THEN 'Saturday'
    END AS created_dow
  , FORMAT_DATE('%Y-%V', o.first_test_order_date) AS first_test_order_week
  , FLOOR(GREATEST(DATE_DIFF(o.created_date, o.first_test_order_date, DAY), 0)/7) AS _week_num_after_first_test_order
  , en.region
  , en.brand_name
  , en.country_name
FROM rps_client_orders_agg o
INNER JOIN entities en ON o.entity_id = en.entity_id
