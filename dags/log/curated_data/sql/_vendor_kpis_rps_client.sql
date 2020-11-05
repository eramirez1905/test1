CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._vendor_kpis_rps_client`
PARTITION BY created_date_local
CLUSTER BY entity_id, vendor_code AS
WITH rps_orders AS (
  SELECT *
  FROM `{{ params.project_id }}.cl.rps_orders`
  WHERE delivery_type = 'VENDOR_DELIVERY'
), rps_vendor_client_events_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl.rps_vendor_client_events`
), order_events AS (
  SELECT orders.id AS order_id
  , ARRAY_AGG(
      STRUCT(
        created_at
        , event_name
        , orders.minutes AS order_minutes
      )
    ) AS events
  FROM rps_vendor_client_events_dataset
  WHERE event_name = 'order_accepted'
    AND orders.minutes IS NOT NULL
  GROUP BY 1
), order_events_agg AS (
  SELECT created_hour_local
    , entity_id
    , vendor_code
    , SUM(order_minutes) AS total_promised_delivery_time_client
  FROM (
    SELECT DATETIME_TRUNC(DATETIME(o.created_at, o.timezone), HOUR) AS created_hour_local
      , o.entity.id AS entity_id
      , o.vendor.code AS vendor_code
      , o.order_id
      , (SELECT order_minutes FROM UNNEST(ev.events) ORDER BY created_at LIMIT 1) AS order_minutes
    FROM rps_orders o
    INNER JOIN order_events ev ON o.order_id = ev.order_id
  )
  GROUP BY 1,2,3
), client_events_agg AS (
  SELECT DATETIME_TRUNC(DATETIME(created_at, timezone), HOUR) AS created_hour_local
    , entity_id
    , vendor_code
    , COUNTIF(event_name = 'login_started') AS login_started_ct
    , COUNTIF(event_name = 'login_succeeded') AS login_succeeded_ct
    , COUNTIF(event_name = 'login_failed') AS login_failed_ct
  FROM rps_vendor_client_events_dataset
  WHERE event_name IN ('login_started', 'login_succeeded', 'login_failed')
  GROUP BY 1,2,3
), vendor_dates_base AS (
  SELECT created_hour_local
    , entity_id
    , vendor_code
  FROM order_events_agg
  UNION DISTINCT
  SELECT created_hour_local
    , entity_id
    , vendor_code
  FROM client_events_agg
)
SELECT CAST(b.created_hour_local AS DATE) AS created_date_local
  , b.created_hour_local
  , b.entity_id
  , b.vendor_code
  , COALESCE(o.total_promised_delivery_time_client, 0) AS total_promised_delivery_time_client
  , COALESCE(ev.login_started_ct, 0) AS login_started_ct
  , COALESCE(ev.login_succeeded_ct, 0) AS login_succeeded_ct
  , COALESCE(ev.login_failed_ct, 0) AS login_failed_ct
FROM vendor_dates_base b
LEFT JOIN order_events_agg o ON b.vendor_code = o.vendor_code
  AND b.entity_id = o.entity_id
  AND b.created_hour_local = o.created_hour_local
LEFT JOIN client_events_agg ev ON b.vendor_code = ev.vendor_code
  AND b.entity_id = ev.entity_id
  AND b.created_hour_local = ev.created_hour_local
