-- query to export from pg db
SELECT
  e.country_code,
  CASE 
    WHEN e.event_type = 'shrinking_polygon' THEN 'shrink' 
    WHEN e.event_type = 'closure' THEN 'close' 
    WHEN e.event_type = 'delay' THEN 'delay'
  -- cast to text as dwh.events.event_type is of type enum
    ELSE e.event_type::text
  END AS event_type,
  -- use external_id instead of dwh-internal id
  e.external_id AS event_id,
  e.starts_at,
  e.ends_at,
  e.value,
  e.geo_object_type,
  e.geo_object_id,
  ST_AsText(e.geo_json) AS geom
FROM dwh.events e
WHERE 
  e.country_code NOT IN ('fr', 'nl', 'it', 'au') AND
  -- ignore events that have neither geometry nor geo id
  (e.geo_json IS NOT NULL OR e.geo_object_id IS NOT NULL)
