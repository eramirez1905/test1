CREATE OR REPLACE TABLE forecasting.events_to_zones
PARTITION BY created_date
CLUSTER BY country_code AS
WITH 
geometries_to_zones AS
(
  SELECT
    e.country_code,
    e.event_id,
    e.event_type,
    z.zone_id,
    ROUND(ST_AREA(ST_INTERSECTION(e.geom, z.geom)) / ST_AREA(z.geom), 3) AS event_overlaps_zone,
    ROUND(ST_AREA(ST_INTERSECTION(e.geom, z.geom)) / ST_AREA(e.geom), 3) AS zone_overlaps_event
  FROM forecasting.events e
  LEFT JOIN forecasting.zones z
    ON z.country_code = e.country_code 
    AND ST_INTERSECTS(e.geom, z.geom)
  WHERE 
    e.geo_object_type = 'country'
    AND e.geom IS NOT NULL
),
geometry_events_to_zones AS
(
  SELECT
    e.country_code,
    e.event_type,
    e.event_id,
    gz.zone_id,
    gz.event_overlaps_zone,
    gz.zone_overlaps_event,
    CASE 
      WHEN gz.event_overlaps_zone > 0.25 THEN 'zone'
      WHEN gz.event_overlaps_zone BETWEEN 0.15 AND 0.25 AND gz.zone_overlaps_event > 0.15 THEN 'intersection'
      WHEN gz.event_overlaps_zone < 0.15 THEN 'unrelated'
    END AS relation
  FROM forecasting.events e
  LEFT JOIN geometries_to_zones gz
    ON gz.country_code = e.country_code
    AND gz.event_type = e.event_type
    AND gz.event_id = e.event_id
  WHERE 
    e.geo_object_type = 'country'
    AND e.geom IS NOT NULL
),
non_geometry_events_to_zones AS
(
  SELECT
    e.country_code,
    e.event_type,
    e.event_id,
    z.zone_id,
    geo_object_type AS relation
  FROM forecasting.events e
  LEFT JOIN (SELECT DISTINCT country_code, city_id FROM forecasting.zones) c 
    ON c.country_code = e.country_code 
    AND CASE WHEN e.geo_object_type = 'city' THEN c.city_id = e.geo_object_id ELSE TRUE END
  LEFT JOIN forecasting.zones z 
    ON z.country_code = c.country_code
    AND z.city_id = c.city_id
  WHERE   
    e.geo_object_type IN ('city', 'country')
    AND e.geom IS NULL
),
events_to_zones AS
(
  SELECT
    country_code,
    event_type,
    event_id,
    zone_id,
    event_overlaps_zone,
    zone_overlaps_event,
    relation
  FROM geometry_events_to_zones
  UNION ALL
  SELECT
    country_code,
    event_type,
    event_id,
    zone_id,
    NULL AS event_overlaps_zone,
    NULL AS zone_overlaps_event,
    relation
  FROM non_geometry_events_to_zones
)
SELECT
  e.country_code,
  e.event_type,
  e.event_id,
  e2z.event_overlaps_zone,
  e2z.zone_overlaps_event,
  e2z.relation,
  COALESCE(e2z.zone_id, 0) AS zone_id,
  DATE(e.starts_at) AS created_date
FROM forecasting.events e
LEFT JOIN events_to_zones e2z 
  ON e2z.country_code = e.country_code
  AND e2z.event_type = e.event_type
  AND e2z.event_id = e.event_id 
  AND e2z.relation != 'unrelated'
