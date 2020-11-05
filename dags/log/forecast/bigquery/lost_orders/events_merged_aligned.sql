CREATE OR REPLACE TABLE `lost_orders.events_merged_aligned` AS
WITH events AS (
SELECT
  e.country_code,
  e.event_type,
  ez.zone_id,
  e.event_id,
  e.starts_at,
  e.ends_at
FROM `forecasting.events_to_zones` AS ez
LEFT JOIN `forecasting.events` AS e 
  ON e.country_code = ez.country_code
  AND e.event_type = ez.event_type
  AND e.event_id = ez.event_id
WHERE 
  (
    -- use thresholds for shrink events
    CASE 
      WHEN e.event_type = 'shrink_legacy' THEN e.value >= 20
      WHEN e.event_type = 'shrink' THEN e.value <= 12
      ELSE TRUE
    END
    )
  AND (
    -- some events are sent without geometries but for the whole city (shrink_legacy)
    ez.relation = 'city'
    OR 
    ez.event_overlaps_zone > .3 
    OR 
    (ez.zone_overlaps_event > .3 AND ez.event_overlaps_zone > .6)
    )
  AND ez.zone_id > 0
  -- duration between 4 minutes and 25 hours.
  AND (UNIX_SECONDS(e.ends_at) - UNIX_SECONDS(e.starts_at))/ 60 BETWEEN 4 AND (25*60)
 ),
aligned_events AS (
SELECT 
  country_code,
  zone_id,
  event_id,
  event_type,
  time_start,
  starts_at,
  ends_at
FROM events e
CROSS JOIN 
  UNNEST(
    GENERATE_TIMESTAMP_ARRAY(
      CASE
        WHEN EXTRACT(MINUTE from ends_at) < 30 THEN
          TIMESTAMP_ADD(TIMESTAMP(DATE(ends_at)),INTERVAL EXTRACT(HOUR from ends_at) HOUR)
        ELSE TIMESTAMP_ADD(TIMESTAMP(DATE(ends_at)),INTERVAL EXTRACT(HOUR from ends_at)+1 HOUR)
      END,
      TIMESTAMP_ADD(TIMESTAMP(DATE(e.starts_at)),INTERVAL EXTRACT(HOUR from e.starts_at) HOUR),
      INTERVAL -30 MINUTE
      )
    ) AS time_start
),
event_triggers AS (
SELECT 
  *,
  lag(ends_at) OVER (partition BY country_code, zone_id, event_type, time_start ORDER BY starts_at) < starts_at 
    OR NULL AS event_trigger
FROM aligned_events
),
overlapped_events AS (
SELECT 
  *,
  COUNT(event_trigger) over (partition BY country_code, zone_id, event_type, time_start ORDER BY starts_at) AS event_overlap
FROM event_triggers
)
SELECT 
  country_code,
  zone_id,
  event_type,
  event_id_merged,
  CASE
    WHEN merged_event_start < datetime_starts_at THEN datetime_starts_at
    ELSE merged_event_start
  END AS starts_at,
  CASE
    WHEN merged_event_end > datetime_ends_at THEN datetime_ends_at
    ELSE merged_event_end
  END AS ends_at,
  datetime_starts_at,
  datetime_ends_at,
  merged_event_start,
  merged_event_end
FROM (
  SELECT 
    country_code,
    zone_id,
    event_type,
    time_start,
    event_overlap,
    STRING_AGG(event_id,'_') AS event_id_merged,
    MIN(starts_at) AS merged_event_start,
    MAX(ends_at) AS merged_event_end,
    DATE(MIN(starts_at)) AS DAY,
    time_start AS datetime_starts_at,
    TIMESTAMP_ADD(time_start, INTERVAL 30 MINUTE) AS datetime_ends_at
  FROM overlapped_events
  GROUP BY 1,2,3,4,5
)
