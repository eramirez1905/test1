CREATE OR REPLACE TABLE rl.scorecard_infra_zones_shapes AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 4 WEEK) AS start_time
    , DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY) AS end_time
-------------------- VENDOR DENSITY --------------------
), days AS (
   SELECT GENERATE_DATE_ARRAY((SELECT start_time FROM parameters), (SELECT end_time FROM parameters), INTERVAL 1 DAY) AS day
), day_sequence AS (
    SELECT c.country_code
    , c.city_id
    , c.zone_id
    , day
  FROM (
    SELECT pc.country_code
      , ci.id AS city_id
      , z.id AS zone_id
    FROM cl.countries pc
    LEFT JOIN UNNEST(cities) ci
    LEFT JOIN UNNEST(zones) z
  ) c
  CROSS JOIN days
  LEFT JOIN UNNEST(days.day) AS day
), deliveries_to_zones AS (
  SELECT o.country_code
    , d.city_id
    , o.timezone
    , o.zone_id
    , o.order_id
    , vendor.id AS vendor_id
    , d.id AS delivery_id
    , d.rider_dropped_off_at
  FROM cl.orders o
  LEFT JOIN UNNEST(deliveries) d
  WHERE o.created_date >= (SELECT start_time FROM parameters)
    AND CAST(DATETIME(d.rider_dropped_off_at, d.timezone) AS DATE) BETWEEN (SELECT start_time FROM parameters) AND (SELECT end_time FROM parameters) --- Consider all vendors having completed
    -- at least one order in the past 4 weeks
    AND d.delivery_status = 'completed'
), delivery_areas AS (
  SELECT dz.country_code
    , dz.city_id
    , dz.zone_id
    , ST_UNION_AGG((SELECT shape FROM d.history WHERE CAST(active_from AS DATE) BETWEEN (SELECT start_time FROM parameters) AND (SELECT end_time FROM parameters) ORDER BY active_to DESC LIMIT 1)) AS zone_shape
  FROM deliveries_to_zones dz
  LEFT JOIN cl.vendors v ON dz.country_code = v.country_code
    AND dz.vendor_id = v.vendor_id
  LEFT JOIN UNNEST(v.delivery_areas) d
  WHERE ('platform_delivery' IN UNNEST(delivery_provider)
      OR 'Hurrier' IN UNNEST(delivery_provider))
    AND d.is_deleted IS FALSE
  GROUP BY 1,2,3
), zone_geo AS (
  SELECT country_code
    , ci.id AS city_id
    , z.id AS zone_id
    , ST_UNION_AGG(z.shape) AS zone_geog
  FROM cl.countries co
  LEFT JOIN UNNEST(cities) ci
  LEFT JOIN UNNEST(zones) z
  GROUP BY 1, 2, 3
)
SELECT z.country_code
  , z.city_id
  , z.zone_id
  , ST_AREA(COALESCE(z.zone_shape, ci.zone_geog))/1000000 AS area ----- For all cities who have not migrated to Porygon, fill the nulls with cl.countries
FROM delivery_areas z
LEFT JOIN zone_geo ci ON z.country_code = ci.country_code
  AND z.city_id = ci.city_id
  AND z.zone_id = ci.zone_id
