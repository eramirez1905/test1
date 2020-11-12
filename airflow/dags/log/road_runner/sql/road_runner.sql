WITH orders AS (
  SELECT c.country_code
    , o.city_id
    , o.timezone
    , o.created_at
    , o.created_date
    , o.vendor.location AS vendor_location
    , o.deliveries
    , z.geo_id
    , sp.id AS starting_point_id
  FROM cl.orders o 
  LEFT JOIN cl.countries c ON c.country_code = o.country_code
  LEFT JOIN UNNEST(cities) ci ON ci.id = o.city_id
  LEFT JOIN UNNEST(ci.zones) z ON z.id = o.zone_id
  LEFT JOIN UNNEST(z.starting_points) sp ON sp.zone_id = o.zone_id
  WHERE o.created_date BETWEEN DATE_SUB('{{ ds }}', INTERVAL 2 DAY) AND '{{ ds }}'
), time_dataset AS (
  SELECT ts
    , country_code 
  FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(TIMESTAMP_SUB('{{ ds }}', INTERVAL 2 DAY), '{{ ds }}', INTERVAL 1 HOUR)) ts
  CROSS JOIN `cl.countries` 
  WHERE country_code IS NOT NULL
)
, dataset AS (
  SELECT o.country_code
    , DATETIME(ts) AS ts
    , o.city_id
    , o.geo_id
    , o.starting_point_id
    , ST_GEOHASH(o.vendor_location, 8) AS location
    , SUM((SELECT COUNT(*) FROM o.deliveries WHERE delivery_status = 'completed')) AS mag
  FROM time_dataset td
  LEFT JOIN orders o ON DATETIME(o.created_at, o.timezone) >= DATETIME(ts, o.timezone)
    AND DATETIME(o.created_at, o.timezone) < DATETIME(TIMESTAMP_ADD(ts, INTERVAL 1 HOUR), o.timezone)
    AND td.country_code = o.country_code
  GROUP BY 1,2,3,4,5,6
)
SELECT * EXCEPT (location)
  , ST_ASGEOJSON(ST_GEOGPOINTFROMGEOHASH(location)) AS location
  , ST_X(ST_GEOGPOINTFROMGEOHASH(location)) as longitude
  , ST_Y(ST_GEOGPOINTFROMGEOHASH(location)) as latitude
FROM dataset
WHERE country_code IS NOT NULL
  AND location IS NOT NULL
