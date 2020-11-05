CREATE OR REPLACE VIEW `{{ params.project_id }}.{{ params.feature_store_dataset }}.{{ params.view_name }}` AS
WITH dataset AS (
  SELECT
    CASE
      WHEN global_entity_id = 'FO_HK' THEN 'FP_HK' -- In DataFridge we see Foodora orders, the last one from 2018-12-12.
                                                   -- Yet, in cl.orders we do not have FO_HK as it was integrated/merged
                                                   -- into Foodpanda (FP_HK).
      WHEN global_entity_id = 'FO_SG' THEN 'FP_SG' -- In DataFridge we see Foodora orders, the last one from 2018-10-11.
                                                   -- Yet, in cl.orders we do not have FO_SG as it was integrated/merged
                                                   -- into Foodpanda (FP_SG).
      ELSE global_entity_id
    END AS global_entity_id
    , content.vendor.id AS vendor_id
    , content.order_id
    , ARRAY_AGG(
        STRUCT(
           timestamp AS created_at
          , content.order_id
          , content.status
          , metadata.source
        ) ORDER BY content.timestamp
      ) AS orders
  FROM `{{ params.project_id }}.dl.data_fridge_order_status_stream`
  WHERE
    content.status IN ('ACCEPTED'
                          , 'PICKED_UP'
                          , 'DELIVERED'
                          , 'CANCELLED'
                          , 'EXPIRED'
                          , 'REJECTED')
    AND metadata.source NOT IN ('fridge-sli-service')
    {%- if params.real_time %}
    AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
    {%- elif params.daily %}
    AND CAST(timestamp AS DATE) BETWEEN DATE_SUB('{{ ds }}', INTERVAL 1 DAY) AND '{{ next_ds }}'
    {%- endif %}
  GROUP BY 1,2,3
), dataset_with_accepted AS (
  SELECT global_entity_id
    , vendor_id
    , order_id
    , (SELECT MIN(created_at) FROM d.orders WHERE status = 'ACCEPTED') AS accepted_at
    , d.orders
  FROM dataset d
), dataset_with_closed AS (
  SELECT global_entity_id
    , vendor_id
    , order_id
    , accepted_at
    , COALESCE(
        (SELECT MIN(created_at) FROM d.orders WHERE status IN ('PICKED_UP', 'DELIVERED', 'CANCELLED', 'EXPIRED', 'REJECTED'))
        , TIMESTAMP_ADD(accepted_at, INTERVAL 60 MINUTE)
      ) AS closed_at
    , d.orders
  FROM dataset_with_accepted d
), dataset_ts AS(
  SELECT global_entity_id
    , vendor_id
    , order_id
    , TIMESTAMP_TRUNC(accepted_at, MINUTE) AS accepted_at
    , TIMESTAMP_TRUNC(closed_at, MINUTE) AS closed_at
    , orders
  FROM dataset_with_closed
  WHERE accepted_at <= closed_at
)
, dataset_final AS (
  SELECT timestamp_grid
    , global_entity_id
    , vendor_id
    , order_id
    , orders
  FROM dataset_ts
  LEFT JOIN UNNEST(
    GENERATE_TIMESTAMP_ARRAY(
      TIMESTAMP_TRUNC(accepted_at, MINUTE),
      TIMESTAMP_TRUNC(closed_at, MINUTE),
      INTERVAL 1 MINUTE
    )
  ) AS timestamp_grid
)
SELECT CAST(timestamp_grid AS DATE) AS created_date
  , timestamp_grid AS timestamp
  , global_entity_id
  , vendor_id
  , COUNT(DISTINCT order_id) AS orders_queued
FROM dataset_final
GROUP BY 1,2,3,4
