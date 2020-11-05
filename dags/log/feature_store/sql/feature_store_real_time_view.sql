--------------------
---live orders----
--------------------
CREATE OR REPLACE VIEW `{{ params.project_id }}.{{ params.feature_store_dataset }}.{{ params.view_name }}` AS
WITH raw_orders AS (
  SELECT * EXCEPT(is_first_replica)
  FROM (
    SELECT TIMESTAMP_TRUNC(`timestamp`, MINUTE) AS received_at
      , content.order_id
      , LOWER(content.country_code) AS iso_country_code
      , content.global_entity_id AS entity_id
      , content.vendor.id AS vendor_code
      -- Latitude and longitude are switched in data fridge!
      , ST_GEOGPOINT(content.delivery.location.longitude, content.delivery.location.latitude) AS dropoff_location
      , ROW_NUMBER() OVER(
          PARTITION BY content.country_code
            , content.order_id
          ORDER BY content.timestamp
        ) = 1 AS is_first_replica
    FROM `{{ params.project_id }}.dl.data_fridge_order_stream`
    WHERE (content.preorder IS NULL OR NOT content.preorder)
      AND (content.test_order IS NULL OR NOT content.test_order)
      AND (content.delivery.provider IS NULL OR content.delivery.provider = 'platform_delivery')
      AND content.country_code IS NOT NULL
      AND content.global_entity_id IS NOT NULL
      AND content.vendor.id IS NOT NULL
      AND content.order_id IS NOT NULL
      {%- if params.real_time %}
      AND `timestamp` BETWEEN TIMESTAMP_SUB(TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 MINUTE), MINUTE), INTERVAL 31 MINUTE)
        AND TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 MINUTE), MINUTE)
      {%- elif params.daily %}
      AND CAST(timestamp AS DATE) BETWEEN DATE_SUB('{{ ds }}', INTERVAL 1 DAY) AND '{{ next_ds }}'
      {%- endif %}
  )
  WHERE is_first_replica
), vendors AS (
  SELECT entity_id
   , vendor_code
   , COALESCE(location, last_provided_location) AS pickup_location
   , COALESCE((SELECT is_halal FROM UNNEST(porygon) ORDER BY updated_at DESC LIMIT 1), FALSE) AS is_halal
   , (SELECT country_code FROM UNNEST(hurrier) WHERE last_provided_location IS NOT NULL ORDER BY updated_at DESC LIMIT 1) AS hurrier_country_code
  FROM `{{ params.project_id }}.cl.vendors_v2`
), orders AS (
  SELECT raw_orders.* EXCEPT(iso_country_code, entity_id, vendor_code)
    , pickup_location
    , is_halal
    , COALESCE(vendors.hurrier_country_code, raw_orders.iso_country_code) AS country_code
    , ST_DISTANCE(dropoff_location, pickup_location) AS delivery_distance
  FROM raw_orders
  JOIN vendors USING (entity_id, vendor_code)
), zones AS (
  SELECT country_code
    , zone.id AS zone_id
    , zone.shape
    , COALESCE(zone.distance_cap, CAST('inf' AS float64)) AS distance_cap
    , 'halal' IN UNNEST(zone.delivery_types) AS is_halal
    , zone.area
    , zone.boundaries
    , zone.is_embedded
 FROM `{{ params.project_id }}.cl.countries`
 LEFT JOIN UNNEST(cities) AS city
 LEFT JOIN UNNEST(zones) AS zone
 WHERE zone.is_active
   AND country_code IS NOT NULL
   AND zone.shape IS NOT NULL
   AND zone.area IS NOT NULL
   AND zone.boundaries IS NOT NULL
   AND zone.is_embedded IS NOT NULL
), orders_to_zones AS (
  SELECT orders.country_code
    , orders.order_id
    , orders.is_halal
    , COALESCE(zones.zone_id, 0) AS zone_id
    , COALESCE(ST_CONTAINS(zones.shape, orders.dropoff_location), FALSE) AS is_dropoff_zone
    , COALESCE(ST_CONTAINS(zones.shape, orders.pickup_location), FALSE) AS is_pickup_zone
    , COALESCE(ROUND(ST_DISTANCE(orders.dropoff_location, ST_CLOSESTPOINT(zones.boundaries, orders.dropoff_location)), 1), CAST('-inf' AS float64)) AS dropoff_to_boundary_distance
    , zones.distance_cap
    , zones.area
    , zones.is_embedded AS is_embedded_zone
  FROM orders
  LEFT JOIN zones ON zones.country_code = orders.country_code
    AND IF(orders.country_code = 'my', zones.is_halal = orders.is_halal, TRUE)
    AND (orders.dropoff_location IS NOT NULL OR orders.pickup_location IS NOT NULL)
    AND (ST_CONTAINS(zones.shape, orders.dropoff_location) OR ST_CONTAINS(zones.shape, orders.pickup_location))
    AND COALESCE(orders.delivery_distance <= zones.distance_cap, FALSE)
), orders_to_zones_ranked AS (
  SELECT country_code
    , order_id
    , zone_id
    , ROW_NUMBER() OVER (
        PARTITION BY country_code
          , order_id
        ORDER BY is_dropoff_zone DESC
          , is_dropoff_zone AND is_pickup_zone DESC
          , distance_cap ASC
          , is_embedded_zone DESC
          , dropoff_to_boundary_distance DESC
          , area ASC
          , zone_id ASC
      ) AS rank
  FROM orders_to_zones
), mapped_orders AS (
  SELECT orders.order_id
    , orders.country_code
    , orders_to_zones_ranked.zone_id
  FROM orders
  JOIN orders_to_zones_ranked ON orders_to_zones_ranked.country_code = orders.country_code
    AND orders_to_zones_ranked.order_id = orders.order_id
    AND orders_to_zones_ranked.rank = 1
), live_orders AS (
  SELECT country_code
    , zone_id
    , COUNT(order_id) AS n_orders
  FROM mapped_orders
  GROUP BY 1, 2
)
--------------------
---queued orders----
--------------------
, dataset AS (
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
    AND timestamp >= TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR), MINUTE)
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
), queued_orders_aggregate AS (
  SELECT CAST(timestamp_grid AS DATE) AS created_date
    , timestamp_grid AS timestamp
    , global_entity_id
    , vendor_id
    , COUNT(DISTINCT order_id) AS orders_queued
  FROM dataset_final
  GROUP BY 1,2,3,4
), queued_orders AS (
  SELECT *
  FROM queued_orders_aggregate
  {%- if params.real_time %}
  WHERE timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 MINUTE) AND CURRENT_TIMESTAMP()
  {%- endif %}
)
SELECT created_date
  , timestamp AS queued_orders_timestamp
  , global_entity_id
  , vendor_id
  , orders_queued
  , null AS country_code
  , null AS zone_id
  , null AS n_orders
  , null AS live_orders_timestamp
  , 'queued_orders' AS type
FROM queued_orders
UNION ALL
SELECT null AS created_date
  , null AS queued_orders_timestamp
  , null AS global_entity_id
  , null AS vendor_id
  , null AS orders_queued
  , country_code
  , zone_id
  , n_orders
  , TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MINUTE) AS live_orders_timestamp
  , 'live_orders' AS type
FROM live_orders
