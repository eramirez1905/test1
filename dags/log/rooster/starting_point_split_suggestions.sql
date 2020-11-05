CREATE OR REPLACE TABLE `staffing.starting_point_split_suggestions`
PARTITION BY created_date
CLUSTER BY country_code
AS
WITH
addresses AS
(
  SELECT
    a.country_code,
    a.id AS address_id,
    a.longitude,
    a.latitude
  FROM `dl.hurrier_addresses` a
  WHERE
    -- remove corrupted geocoords
    a.latitude BETWEEN -90 and 90
),
starting_points AS
(
  SELECT
    c.country_code,
    ci.id AS city_id,
    ci.name AS city_name,
    z.id AS zone_id,
    z.name AS zone_name,
    sp.id  AS starting_point_id,
    sp.name AS starting_point_name,
    sp.updated_at,
    sp.created_at,
    sp.shape AS geom,
    ST_CENTROID(sp.shape) AS centroid_geom,
    ST_BOUNDARY(sp.shape) AS boundary_geom,
    fsp.demand_distribution
  FROM `cl.countries` c
  LEFT JOIN UNNEST(cities) ci
  LEFT JOIN UNNEST(zones) z
  LEFT JOIN UNNEST(starting_points) sp
  LEFT JOIN `dl.forecast_starting_points` fsp
    ON fsp.country_code = c.country_code
    AND fsp.external_id = sp.id
  WHERE
    z.is_active IS TRUE
    AND sp.is_active IS TRUE
),
orders AS
(
  SELECT
    o.country_code,
    o.id AS order_id,
    o.created_at,
    a.longitude,
    a.latitude
  FROM `dl.hurrier_orders` o
  LEFT JOIN `dl.hurrier_deliveries` d
    ON d.country_code = o.country_code
   AND d.order_id = o.id
   -- only use primary deliveries to not produce duplicate rows (ex.: order_id 3619031 in my)
   AND d.is_primary
   -- for idempotency
  LEFT JOIN addresses a
    ON a.country_code = d.country_code
    AND a.address_id = COALESCE(d.pickup_address_id, d.dropoff_address_id)
  WHERE
    o.created_date >= DATE_SUB(DATE('{{next_ds}}'), INTERVAL 28 DAY)
),
-- calculate distance of order to all starting points of mapped zone (cl._orders_to_zones)
distances AS
(
  SELECT
    o.country_code,
    o.order_id,
    o2z.zone_id,
    sp.starting_point_id,
    ROUND(ST_DISTANCE(ST_GEOGPOINT(o.longitude, o.latitude), sp.boundary_geom) / 100) * 100 AS distance
  FROM orders o
  LEFT JOIN `cl._orders_to_zones` o2z USING (country_code, order_id)
  LEFT JOIN starting_points sp USING (country_code, zone_id)
  WHERE
    o2z.zone_id IS NOT NULL
),
-- rank by ascending distance
starting_point_ranks AS
(
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY country_code, order_id ORDER BY distance ASC, starting_point_id ASC) AS row_number
  FROM distances
),
starting_point_aggregates AS
(
  SELECT
    country_code,
    zone_id,
    starting_point_id,
    COUNT(*) AS orders_sum,
    ROUND(AVG(CASE WHEN distance < 15000 THEN distance ELSE NULL END) / 1000, 2) AS distance_avg
  FROM starting_point_ranks
  WHERE
    row_number = 1
  GROUP BY 1,2,3
),
splits_suggested AS
(
  SELECT
    sp.country_code,
    sp.city_id,
    sp.zone_id,
    sp.starting_point_id,
    sp.created_at AS starting_point_created_at,
    sp.updated_at AS starting_point_updated_at,
    a.orders_sum,
    sp.demand_distribution AS split,
    ROUND(a.orders_sum / SUM(a.orders_sum) OVER (PARTITION BY a.country_code, a.zone_id), 2) AS split_suggested,
    a.distance_avg
  FROM starting_points sp
  LEFT JOIN starting_point_aggregates a USING (country_code, starting_point_id)
  ORDER BY 1,2,3,4
)
SELECT
  country_code,
  city_id,
  zone_id,
  starting_point_id,
  starting_point_created_at,
  starting_point_updated_at,
  orders_sum,
  split,
  COALESCE(split_suggested, 0) AS split_suggested,
  distance_avg,
  DATE('{{next_ds}}') AS created_date
FROM splits_suggested
