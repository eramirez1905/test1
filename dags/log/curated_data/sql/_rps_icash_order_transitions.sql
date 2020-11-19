-- We get last two days here (instead of one) because we do aggregation at the end of the query and we want to get records across midnight
WITH order_status AS (
  SELECT *
  FROM `{{ params.project_id }}.cl._rps_order_status`
), icash_delivery_changelog_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.dl.icash_delivery_changelog`
  {%- if not params.backfill %}
  WHERE created_date BETWEEN DATE_SUB('{{ next_ds }}', INTERVAL 2 DAY) AND '{{ next_ds }}'
  {%- endif %}
), icash_delivery_platform_notification_dataset AS (
  SELECT created_date
    , region
    , delivery_id AS order_id
    , timestamp AS created_at
    , CASE
        WHEN type IN ('TRANSMITTING', 'MISSED', 'EXPIRED')
          THEN 'RPS'
        WHEN type IN ('SENT_TO_POS', 'ACCEPTED', 'REJECTED')
          THEN 'VENDOR_DEVICE'
        WHEN type IN ('ORIGIN_ARRIVED', 'DESTINATION_ARRIVED')
          THEN 'LOGISTICS'
        ELSE NULL
      END AS source
    , CONCAT('NOTIFICATION_', type) AS state
  FROM `{{ params.project_id }}.dl.icash_delivery_platform_notification`
  WHERE type IN ('TRANSMITTING', 'ORIGIN_ARRIVED', 'DESTINATION_ARRIVED', 'ACCEPTED', 'CANCELLED', 'EXPIRED', 'MISSED', 'SENT_TO_POS', 'DELAYED', 'DISPATCHED')
  {%- if not params.backfill %}
    AND created_date BETWEEN DATE_SUB('{{ next_ds }}', INTERVAL 2 DAY) AND '{{ next_ds }}'
  {%- endif %}
), changelogs_raw AS (
  SELECT created_date
    , region
    , delivery_id AS order_id
    , timestamp AS created_at
    , CAST(JSON_EXTRACT_SCALAR(new_value, "$.state") AS INT64) AS state_id
    , CAST(JSON_EXTRACT_SCALAR(new_value, "$.trackingState") AS INT64) AS tracking_state_id
    , STRUCT(
          JSON_EXTRACT_SCALAR(new_value, "$.externalId") AS order_code
        , JSON_EXTRACT_SCALAR(new_value, "$.logisticsDriverId") AS rider_id
      ) AS metadata
    , new_value
    , user
  FROM icash_delivery_changelog_dataset
), changelogs_with_states AS (
  SELECT region
    , order_id
    , created_at
    , created_date
    , (SELECT CONCAT('DELIVERY_', state) FROM order_status, UNNEST(delivery_states) WHERE id = c.state_id) AS delivery_state
    , (SELECT CONCAT('TRACKING_', state) FROM order_status, UNNEST(tracking_states) WHERE id = c.tracking_state_id) AS tracking_state
    , user
    , metadata
    , new_value
  FROM changelogs_raw c
), changelogs AS (
  SELECT cgl.region
    , cgl.created_date
    , cgl.order_id
    , cgl.created_at
    , state
    , CASE
        WHEN cgl.user IN ('amrest_hu', 'appetito24-pa', 'boozer', 'carriage', 'dame-jidlo-cz', 'domicilios_pedidos_ya', 'donesi_rs', 'efood', 'hip_menu_ro', 'mjam', 'mjam_3pl', 'net_pincer_hu', 'pauza_hr', 'pedidos-ya', 'pizza-portal-pl', 'talabat', 'talabat_otlob', 'vapiano1@vapiano.eu', 'walmart_ca', 'yemeksepeti', 'yogiyo_kr_api')
          THEN 'PLATFORM'
        WHEN cgl.user LIKE 'foodora_%'
          THEN 'PLATFORM'
        WHEN cgl.user LIKE 'foodpanda_%'
          THEN 'PLATFORM'
        WHEN cgl.user = 'yemeksepeti_groceries'
          THEN 'SHOPPER'
        WHEN cgl.user LIKE '%@%'
          THEN 'VENDOR_DEVICE'
        WHEN cgl.user = 'HURRIER'
          THEN 'LOGISTICS'
        WHEN cgl.user = 'crs-service-account'
          THEN 'RPS_CLIENT'
        WHEN cgl.user IN ('OutdatedDeliveryCleanupJob', 'ExpireDeliveriesJob')
          THEN 'RPS_CORE'
        WHEN state IN ('DELIVERY_WAITING_FOR_TRANSPORT')
          THEN 'RPS_CORE'
        WHEN cgl.user = 'manager'
          THEN 'RPS_CORE_LEGACY'
        WHEN cgl.user = ''
          THEN '(UNKNOWN)'
        ELSE '(Others)'
      END AS source
    , cgl.metadata
    , cgl.new_value
  FROM changelogs_with_states cgl
  LEFT JOIN UNNEST([delivery_state, tracking_state]) state
  WHERE state IS NOT NULL
), transitions_union AS (
  SELECT region
    , created_date
    , order_id
    , created_at
    , state AS icash_state
    , source
    , metadata
  FROM changelogs
  UNION ALL
  SELECT region
    , created_date
    , order_id
    , created_at
    , state AS icash_state
    , source
    , NULL
  FROM icash_delivery_platform_notification_dataset
), two AS (
  SELECT *
    , EXISTS(SELECT state_temp FROM t.transitions WHERE icash_state = 'DELIVERY_INITIALIZING') AS is_own_delivery
  FROM (
    SELECT tu.region
      , tu.order_id
      , MIN(tu.created_at) AS created_at
      , ARRAY_AGG(
          STRUCT(
            tu.created_at
            , tu.icash_state
            , ARRAY(SELECT f.state FROM os.order_flow f WHERE tu.icash_state IN UNNEST(f.icash_state)) AS state_temp
            , tu.source
            , tu.metadata
          ) ORDER BY tu.created_at
        ) AS transitions
    FROM transitions_union tu
    CROSS JOIN order_status os
    GROUP BY 1,2
  ) t
), three AS (
  SELECT t.region
    , CAST(t.created_at AS DATE) AS created_date
    , t.order_id
    , t.created_at
    , t.is_own_delivery
    , ARRAY(
        SELECT AS STRUCT tt.created_at
          , tt.icash_state
          , IF(tt.icash_state = 'DELIVERY_NEW',
              (
                SELECT dn.states
                FROM os.order_flow_delivery_new dn
                WHERE t.is_own_delivery = dn.is_own_delivery
              ),
              ARRAY(
                SELECT f.state
                FROM os.order_flow f
                WHERE tt.icash_state IN UNNEST(f.icash_state)
              )
            ) AS states
          , tt.source
          , tt.metadata
        FROM t.transitions tt
      ) AS transitions
  FROM two t
  CROSS JOIN order_status os
)
SELECT t.region
  , t.created_date
  , t.order_id
  , t.created_at
  , t.is_own_delivery
  , ARRAY(
      SELECT AS STRUCT tt.created_at
        , (SELECT fs.stage FROM os.order_flow_stages fs WHERE state IN UNNEST(states)) AS stage
        , state
        , CASE
            WHEN state = 'PICKED_UP' AND t.is_own_delivery
              THEN 'LOGISTICS'
            WHEN state = 'PICKED_UP' AND NOT t.is_own_delivery
              THEN 'VENDOR_DEVICE'
            ELSE tt.source
          END AS source
        , CAST(NULL AS STRING) AS owner
        , tt.metadata
      FROM t.transitions tt
      LEFT JOIN UNNEST(tt.states) state
      WHERE ARRAY_LENGTH(tt.states) > 0
      ORDER BY t.created_at
    ) AS transitions
FROM three t
CROSS JOIN order_status AS os
