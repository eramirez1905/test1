CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.rps_gsr_client_dashboard_order_timings_stat`
PARTITION BY created_date
CLUSTER BY entity_id, region, brand_name, country_name AS
WITH entities AS (
  SELECT en.region_short_name AS region
    , en.country_iso
    , LOWER(en.country_iso) AS country_code
    , en.country_name
    , p.entity_id
    , p.brand_name
  FROM `{{ params.project_id }}.cl.entities` en
  CROSS JOIN UNNEST(platforms) p
), rps_orders_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl.rps_orders`
  WHERE created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 1 MONTH)
), rps_orders AS (
  SELECT region
    , order_id
    , order_status = 'cancelled' AS is_fail
    , cancellation.reason AS cancellation_reason
    , cancellation.owner AS cancellation_owner
    , (SELECT SUM(quantity) FROM UNNEST(products)) AS products_ct
    , ARRAY_LENGTH(products) AS _products_ct
  FROM rps_orders_dataset
), rps_vendor_client_events_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl.rps_vendor_client_events`
  WHERE created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 1 MONTH)
), order_events AS (
  SELECT DATE(ev.created_at, ev.timezone) AS created_date
    , ev.region
    , ev.entity_id
    , ev.country_code
    , ev.experiments AS _experiments
    , ev.client.name AS client_name
    , ev.client.version AS client_version
    , COALESCE(ev.client.wrapper_type, '(N/A)') AS wrapper_type
    , COALESCE(ev.client.wrapper_version, '(N/A)') AS wrapper_version
    , COALESCE(ev.device.os_name, '(UNKONWN)') AS os_name
    , COALESCE(ev.device.os_version, '(UNKONWN)') AS os_version
    , COALESCE(ev.device.model, '(UNKONWN)') AS device_model
    , COALESCE(ev.device.brand, '(UNKONWN)') AS device_brand
    , ev.vendor_id
    , ev.vendor_code
    , CONCAT(ev.region , '-', ev.vendor_id ) AS uniqule_vendor_id
    , CONCAT(ev.entity_id , '-', ev.vendor_code ) AS uniqule_vendor_code
    , ev.device.id AS device_id
    , ev.orders.id AS order_id
    , COALESCE(ev.orders.delivery_type, '(UNKONWN)') AS delivery_type
    , COALESCE(orders.is_preorder, FALSE) AS is_preorder
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
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
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
    FROM order_events_clean
  )
), rps_vendor_client_order_events AS (
  SELECT ev.created_date
    , EXTRACT(DAYOFWEEK FROM ev.created_date) AS created_dow
    , ev.region
    , ev.entity_id
    , ev.country_code
    , ev._experiments
    , ev.client_name
    , ev.client_version
    , ev.wrapper_type
    , ev.wrapper_version
    , ev.os_name
    , ev.os_version
    , ev.device_model
    , ev.device_brand
    , ev.vendor_id
    , ev.vendor_code
    , CONCAT(ev.region , '-', ev.vendor_id ) AS uniqule_vendor_id
    , CONCAT(ev.entity_id , '-', ev.vendor_code ) AS uniqule_vendor_code
    , ev.device_id
    , ev.order_id
    , ev.delivery_type
    , ev.is_preorder
    , ot.order_promised_for
    , ot.response_time_client
    , ot.dispatch_time
    , ROW_NUMBER() OVER (PARTITION BY ev.order_id ORDER BY created_at) AS _row_number_per_order
  FROM order_events_clean ev
  LEFT JOIN order_with_timestamps ot ON ev.order_id = ot.order_id
)
SELECT oev.created_date
  , CASE
      WHEN created_dow = 1
        THEN 'Sunday'
      WHEN created_dow = 2
        THEN 'Monday'
      WHEN created_dow = 3
        THEN 'Tuesday'
      WHEN created_dow = 4
        THEN 'Wednesday'
      WHEN created_dow = 5
        THEN 'Thursday'
      WHEN created_dow = 6
        THEN 'Friday'
      WHEN created_dow = 7
        THEN 'Saturday'
    END AS created_dow
  , EXTRACT(YEAR FROM oev.created_date) AS created_year
  , FORMAT_DATE('%Y-%m', oev.created_date) AS created_month
  , FORMAT_DATE('%Y-%V', oev.created_date) AS created_week
  , EXTRACT(ISOWEEK FROM oev.created_date) AS _created_week_num
  , COALESCE(oev.region, en.region) AS region
  , oev.entity_id
  , en.brand_name
  , oev.country_code
  , en.country_name
  , oev._experiments
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
  , oev.response_time_client
  , oev.dispatch_time
  , uniqule_vendor_id
  , oev.uniqule_vendor_code
  , oev.device_id
  , oev.order_id
FROM rps_vendor_client_order_events oev
INNER JOIN entities en ON oev.entity_id = en.entity_id
LEFT JOIN rps_orders o ON oev.order_id = o.order_id
  AND oev.region = o.region
  AND oev._row_number_per_order = 1
WHERE COALESCE(oev.response_time_client, 0) >= 0
  AND COALESCE(oev.dispatch_time, 0) >=0
