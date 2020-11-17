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
      {%- if params.daily %}
      AND CAST(timestamp AS DATE) BETWEEN DATE_SUB('{{ ds }}', INTERVAL 1 DAY) AND '{{ next_ds }}'
      {%- endif %}
  )
  WHERE is_first_replica
), vendor_locations AS (
  SELECT entity_id
   , vendor_code
   , COALESCE(location, last_provided_location) AS pickup_location
   , COALESCE((SELECT is_halal FROM UNNEST(porygon) ORDER BY updated_at DESC LIMIT 1), FALSE) AS is_halal
  FROM `{{ params.project_id }}.cl.vendors_v2`
), country_codes AS (
  SELECT entity_id
    , vendor_code
    , h.country_code AS hurrier_country_code
    , h.created_at
    , h.updated_at
  FROM `{{ params.project_id }}.cl.vendors_v2`
    , UNNEST(hurrier) AS h
  WHERE hurrier IS NOT NULL
    AND h.country_code IS NOT NULL
    AND h.created_at IS NOT NULL
    AND h.updated_at IS NOT NULL
    AND h.last_provided_location IS NOT NULL
), orders AS (
  SELECT * EXCEPT(is_latest_country_code)
  FROM (
    SELECT raw_orders.* EXCEPT(iso_country_code, entity_id, vendor_code)
      , pickup_location
      , is_halal
      , COALESCE(country_codes.hurrier_country_code, raw_orders.iso_country_code) AS country_code
      , ST_DISTANCE(dropoff_location, pickup_location) AS delivery_distance
      , ROW_NUMBER() OVER(
          PARTITION BY raw_orders.iso_country_code
            , raw_orders.order_id
          ORDER BY country_codes.updated_at DESC
        ) = 1 AS is_latest_country_code
    FROM raw_orders
    JOIN vendor_locations
      USING (entity_id, vendor_code)
    LEFT JOIN country_codes
      ON raw_orders.entity_id = country_codes.entity_id
        AND raw_orders.vendor_code = country_codes.vendor_code
        AND raw_orders.received_at > country_codes.created_at
  )
  WHERE is_latest_country_code
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
    , COALESCE(ST_CONTAINS( zones.shape, orders.pickup_location ), FALSE) AS is_pickup_zone
    , COALESCE(ROUND(ST_DISTANCE(orders.dropoff_location, ST_CLOSESTPOINT(zones.boundaries, orders.dropoff_location)), 1), CAST('-inf' AS float64)) AS dropoff_to_boundary_distance
    , zones.distance_cap
    , zones.area
    , zones.is_embedded AS is_embedded_zone
  FROM orders
  LEFT JOIN zones
    ON zones.country_code = orders.country_code
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
  SELECT orders.received_at
    , orders.order_id
    , orders.country_code
    , orders_to_zones_ranked.zone_id
  FROM orders
  JOIN orders_to_zones_ranked
    ON orders_to_zones_ranked.country_code = orders.country_code
      AND orders_to_zones_ranked.order_id = orders.order_id
      AND orders_to_zones_ranked.rank = 1
), aggregated AS (
  SELECT queried_at
    , country_code
    , zone_id
    , COUNT(order_id) AS n_orders
  FROM mapped_orders
    , UNNEST(GENERATE_TIMESTAMP_ARRAY(received_at, TIMESTAMP_ADD(received_at, INTERVAL 30 MINUTE), INTERVAL 1 MINUTE)) AS queried_at
  GROUP BY 1, 2, 3
)
SELECT DATE(queried_at) AS created_date
  , queried_at AS created_at
  , country_code
  , zone_id
  , n_orders
FROM aggregated
