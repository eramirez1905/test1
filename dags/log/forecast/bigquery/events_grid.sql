CREATE TEMPORARY FUNCTION round_timestamp(x TIMESTAMP) AS 
(
  TIMESTAMP_ADD(
    TIMESTAMP_TRUNC(x, MINUTE), 
    INTERVAL CAST(ROUND(EXTRACT(SECOND FROM x) / 60) * 60 AS INT64) SECOND
    )
);

CREATE TEMPORARY FUNCTION get_timestamp_difference(x TIMESTAMP, y TIMESTAMP) AS 
(
  ROUND(TIMESTAMP_DIFF(x, y, SECOND) / 60, 1) 
);

CREATE OR REPLACE TABLE forecasting.events_grid
PARTITION BY created_date
CLUSTER BY country_code, zone_id 
AS
WITH 
events AS 
(
  SELECT 
    e.country_code,
    z.zone_id,
    e.event_type,
    e.event_id,
    -- round to full minutes
    round_timestamp(e.starts_at) AS starts_at,
    round_timestamp(e.ends_at) AS ends_at,
    e.value,
    get_timestamp_difference(e.ends_at, e.starts_at) AS duration,
    e.is_pandora_legacy_event
  FROM forecasting.events e
  LEFT JOIN forecasting.events_to_zones z USING (country_code, event_type, event_id)
)
SELECT
  country_code,
  -- this grid can also have platform in the resolution
  --platform,
  zone_id,
  event_type,
  datetime,
  -- the worst 'value' per resolution window (datetime) should be considered:
  -- * close: closed time, the higher the worse,
  -- * delay: delay time displayed in front end, the higher the worse,
  -- * shrink: the lower the worse, since porygon shrinking is to absolute minute drive time polygons,
  -- * shrink_legacy: the higher the worse, since shrinking is by %
  CASE 
    WHEN event_type IN ('closure', 'delay') THEN MAX(value)
    WHEN event_type = 'shrink' THEN MIN(value)
    WHEN event_type = 'shrink_legacy' THEN MAX(value) 
    ELSE MAX(value)
  END AS value,
  COUNT(*) AS n,
  ARRAY_AGG(DISTINCT event_id) AS event_ids,
  DATE(datetime) AS created_date
FROM events, UNNEST(GENERATE_TIMESTAMP_ARRAY(starts_at, ends_at, INTERVAL 1 MINUTE)) AS datetime
GROUP BY country_code, zone_id, event_type, datetime, created_date
