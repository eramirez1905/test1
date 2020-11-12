CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._orders_to_zones_ranked`
PARTITION BY created_date
CLUSTER BY country_code, order_id, zone_id AS
WITH addresses AS (
  SELECT a.country_code
    -- TODO: remove this once addresses table is fixed
    -- this fixes old data for cities that were merged in the past,
    -- i.e. in hk city_id 1 & 4 were merged into city_id 1.
    , CASE
        WHEN a.country_code = 'hk' and a.city_id = 4    THEN  1
        WHEN a.country_code = 'my' and a.city_id = 3    THEN  203
        WHEN a.country_code = 'my' and a.city_id = 1    THEN  203
        WHEN a.country_code = 'my' and a.city_id = 2    THEN  203
        WHEN a.country_code = 'ph' and a.city_id = 4    THEN  3
        WHEN a.country_code = 'sg' and a.city_id = 4    THEN  200
        WHEN a.country_code = 'sg' and a.city_id = 1    THEN  200
        WHEN a.country_code = 'sg' and a.city_id = 5    THEN  200
        WHEN a.country_code = 'sg' and a.city_id = 3    THEN  200
        WHEN a.country_code = 'th' and a.city_id = 201  THEN  202
        WHEN a.country_code = 'th' and a.city_id = 2    THEN  1
        WHEN a.country_code = 'tw' and a.city_id = 200  THEN  201
        ELSE a.city_id
      END AS city_id
    , a.id AS address_id
    , SAFE.ST_GEOGPOINT(a.longitude, a.latitude) AS shape
  FROM `{{ params.project_id }}.dl.hurrier_addresses` a
), zones AS (
  SELECT c.country_code
    , ci.id AS city_id
    , z.id AS zone_id
    , z.shape
    , 'halal' IN UNNEST(z.delivery_types) AS is_halal
    , 'alcohol' IN UNNEST(z.delivery_types) AS is_alcohol
    , z.distance_cap
    , z.area
    , z.boundaries
    , z.is_embedded
    , z.embedding_zone_ids
  FROM `{{ params.project_id }}.cl.countries` c
  LEFT JOIN UNNEST(cities) ci
  LEFT JOIN UNNEST(zones) z
  WHERE z.is_active IS TRUE
), orders AS (
 SELECT o.country_code
    , o.id AS order_id
    , o.created_at
    , 'halal' IN UNNEST(o.tags) AS is_halal
    , 'signature_proof' IN UNNEST(o.tags) AS is_alcohol
    , d.delivery_distance
    , d.pickup_address_id
    , d.dropoff_address_id
  FROM `{{ params.project_id }}.dl.hurrier_orders` o
  LEFT JOIN `{{ params.project_id }}.dl.hurrier_deliveries` d ON d.country_code = o.country_code
    AND d.order_id = o.id
    -- only use primary deliveries to not produce duplicate rows (ex.: order_id 3619031 in my)
    AND d.is_primary
  -- for idempotency
  WHERE o.created_at <= '{{ next_execution_date }}'
), addresses_to_zones AS (
  SELECT a.country_code
    , a.address_id
    , z.zone_id
    , ST_DISTANCE(a.shape, ST_CLOSESTPOINT(z.boundaries, a.shape)) AS distance_to_boundary
  FROM addresses a
  LEFT JOIN zones z ON z.country_code = a.country_code
    AND z.city_id = a.city_id
    -- this is were the actual geographical matching happens:
    AND ST_CONTAINS(z.shape, a.shape)
  WHERE a.shape IS NOT NULL
), orders_to_zones AS (
  SELECT o.country_code
    , o.order_id
    , o.created_at
    , o.is_halal
    , o.is_alcohol
    , o.delivery_distance
    , z.zone_id
    -- if matched zone has no distance cap, set it to Inf to keep ranking well defined
    , COALESCE(z.distance_cap, CAST('inf' AS float64)) AS distance_cap
    , a_d.zone_id IS NOT NULL AS is_dropoff_zone
    , a_p.zone_id IS NOT NULL AS is_pickup_zone
    , COALESCE(a_d.zone_id = a_p.zone_id, FALSE) AS is_dropoff_zone_equal_to_pickup_zone
    -- if area is null, set it to Inf to keep ranking well defined
    , COALESCE(z.area, CAST('inf' AS float64)) AS area
    , COALESCE(z.is_embedded, FALSE) AS is_embedded_zone
    -- if distance_to_boundary logic should not be used set it to -Inf to keep ranking well defined
    , COALESCE(ROUND(a_d.distance_to_boundary, 1), CAST('-inf' AS float64)) AS dropoff_to_boundary_distance
  FROM orders o
  LEFT JOIN zones z ON o.country_code = z.country_code
  LEFT JOIN addresses_to_zones a_d ON a_d.country_code = o.country_code
   AND a_d.address_id = o.dropoff_address_id
   AND a_d.zone_id = z.zone_id
  LEFT JOIN addresses_to_zones a_p ON a_p.country_code = o.country_code
   AND a_p.address_id = o.pickup_address_id
   AND a_p.zone_id = z.zone_id
  WHERE (a_d.zone_id IS NOT NULL OR a_p.zone_id IS NOT NULL)
    -- in 'my', halal order iff halal zone
    AND CASE WHEN o.country_code = 'my' THEN o.is_halal = z.is_halal ELSE TRUE END
    AND CASE WHEN o.country_code = 'ca' THEN o.is_alcohol = z.is_alcohol ELSE TRUE END
    -- if zone has distance cap, it can only be matched with orders with smaller distance
    AND CASE WHEN z.distance_cap IS NOT NULL THEN o.delivery_distance <= z.distance_cap ELSE TRUE END
)
SELECT country_code
  , order_id
  , zone_id
  , is_dropoff_zone
  , is_pickup_zone
  , is_dropoff_zone_equal_to_pickup_zone
  , distance_cap
  , is_embedded_zone
  , area
  , dropoff_to_boundary_distance
  , is_halal
  , is_alcohol
  -- rank every order to zone match with a business logic to come up with ONE zone per order (i.e. rank=1)
  , ROW_NUMBER() OVER (
      PARTITION BY country_code, order_id
      ORDER BY
        -- in case of multiple matches, rank in following order:
        -- 1.) prefer zones in which the dropoff happened
        is_dropoff_zone DESC,
        -- 2.) prefer zones where dropoff AND pickup happened
        is_dropoff_zone_equal_to_pickup_zone DESC,
        -- 3.) prefer zones with the lower distance cap (if no distance cap is defined it is set to Inf),
        -- which leads to preference of walker zones
        distance_cap ASC,
        -- 4.) prefer embedded zones, which leads to preference of zones which are embedded in other, bigger zones
        -- and are usually used for slower vehicles in city centers, whereas the bigger zone is often served via cars.
        is_embedded_zone DESC,
        -- 5.) prefer zones where dropoff location is further inside polygon (i.e. distance to boundary is highest),
        -- which tries to bring balance to orders fully within overlaps by assigning the zone it's further middle
        dropoff_to_boundary_distance DESC,
        -- 5b.) prefer zones which are smaller
        area ASC,
        -- 6.) prefer zones with lower zone_id (to ensure deterministic behaviour in case of doubt)
        zone_id ASC
    ) AS rank
  , DATE(created_at) AS created_date
FROM orders_to_zones
