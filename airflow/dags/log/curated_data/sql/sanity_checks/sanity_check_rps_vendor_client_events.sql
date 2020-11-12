WITH entities AS (
  SELECT DISTINCT p.entity_id
  FROM  `{{ params.project_id }}.cl.entities` en
  CROSS JOIN UNNEST(platforms) p
), rps_vendor_client_events_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl.rps_vendor_client_events`
), rps_orders AS (
  SELECT DISTINCT order_id
    , entity.id AS entity_id
  FROM `{{ params.project_id }}.cl.rps_orders`
  WHERE client.name IN ('GODROID', 'GOWIN')
    AND created_date BETWEEN DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY) AND '{{ next_ds }}'
), rps_vendor_client_events_orders AS (
  SELECT DISTINCT orders.id AS order_id
    , entity_id
  FROM rps_vendor_client_events_dataset
  WHERE event_scope = 'order'
    AND created_date BETWEEN DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY) AND '{{ next_ds }}'
), rps_orders_with_client_events AS (
  SELECT COUNT(*) = COUNTIF(is_threshold_met) AS referential_integrity_orders_client_events_check
  FROM (
    SELECT o.entity_id
      , COUNT(*) AS orders_ct
      , COUNT(ev.order_id) AS orders_from_client_events_ct
      , SAFE_DIVIDE(COUNT(*) - COUNT(ev.order_id), COUNT(*)) <= 0.01 AS is_threshold_met
    FROM rps_orders o
    -- LEFT JOIN is used here as rps_vendor_client_events could have more order_id than rps_orders, which is acutally a data quality issue caused by lagged tracking events from GoDroid side (i.e. Firebase)
    LEFT JOIN rps_vendor_client_events_orders ev USING(order_id)
    GROUP BY 1
  )
), rps_vendor_client_events AS (
  SELECT COUNT(*) AS count
    , COUNT(ev.entity_id) AS entity_id_count
    , COUNTIF(en.entity_id IS NOT NULL) AS valid_entity_id_count
    , COUNT(ev.event_name) AS event_name_count
    , TIMESTAMP_DIFF('{{ execution_date }}', MAX(ev.created_at), HOUR) AS latest_created_at_diff
  FROM rps_vendor_client_events_dataset ev
  LEFT JOIN entities en ON ev.entity_id = en.entity_id
)
SELECT (ev.count = ev.entity_id_count) AS null_entity_id_check
  , (ev.count = ev.valid_entity_id_count) AS valid_entity_id_check
  , (ev.count = ev.event_name_count) AS unmapped_event_check
  , (ev.latest_created_at_diff <= 24) AS timeliness_check
  , oev.referential_integrity_orders_client_events_check
FROM rps_vendor_client_events ev
CROSS JOIN rps_orders_with_client_events oev
