CREATE OR REPLACE TABLE forecasting.events
PARTITION BY created_date
CLUSTER BY country_code AS
WITH 
pandora_legacy_events AS 
(
  SELECT 
    country_code,
    -- relabel pandora legacy events:
    -- 1.) shrink events as 'shrink_legacy'
    --   since they use a relative shrinking logic (by x%),
    --   compared to the absolute logic porygon uses (to y min driving time)
    -- 2.) manual events as 'outage'
    CASE 
      WHEN event_type = 'shrink' THEN 'shrink_legacy'
      WHEN event_type = 'manual' THEN 'outage'
      ELSE event_type
    END AS event_type,
    event_id,
    starts_at,
    ends_at,
    value,
    geo_object_type,
    geo_object_id,
    geom
  FROM forecasting.events_pandora_legacy --static data source (old pandora dump)
  WHERE 
    starts_at < ends_at
),
-- fetch porygon events of all platforms and deduplicate
porygon_events_raw AS
(
  SELECT
    e.country_code,
    e.platform,
    -- TODO: for now handle lockdowns as closures, as they close customers (as a close does)
    -- and(!) restaurants. downstream this should possibly be handled as an other type of event longterm.
    CASE 
      WHEN t.action = 'lockdown' THEN 'close' 
      ELSE t.action 
    END AS event_type,
    -- create unique id using id (related to polygon) and transaction_id (id of the execution of event)
    CONCAT(CAST(e.event_id AS STRING), '_', CAST(t.transaction_id AS STRING)) AS event_id,
    -- truncate timestamps to seconds because they come in with ms
    TIMESTAMP_TRUNC(t.start_at, SECOND) AS starts_at,
    -- if an event is supposedly ongoing, the ending timestamp does not exist, 
    -- so the ends_at will be set to upper bound of processed timeframe
    COALESCE(TIMESTAMP_TRUNC(t.end_at, SECOND), TIMESTAMP('{{next_execution_date}}')) AS ends_at,
    -- TODO: what happens if in between events alter the value?
    CASE 
      WHEN t.action IN ('close', 'lockdown') THEN 1 
      ELSE t.value 
    END AS value,
    -- porygon events come in as zone events, but zones can change, thus we ignore the zone ids, 
    -- flag them as country events and rely on the events to zones mapping
    'country' AS geo_object_type,
    NULL AS geo_object_id,
    t.shape AS geom
  FROM `fulfillment-dwh-production.cl.porygon_events` e,
  UNNEST(transactions) t
  WHERE
    -- in cl.porygon_events inactive timeframes are stored as well (with is_active=False)
    -- we only consider the active timeframes (the actual events)
    t.is_active
    AND t.action IN ('close', 'shrink', 'delay', 'lockdown')
    -- TODO: this is the hotfix to remove events without parsed geoms (LOGDEP-555)
    AND t.shape IS NOT NULL
    -- sometimes delay events are used to put a banner on frontend, ignore these
    -- (ex.: country_code = 'tw' AND id = 12 AND transaction_id = 9749)
    AND CASE WHEN t.action = 'delay' THEN t.value > 0 ELSE TRUE END
    -- when ends_at (t2.issued_at) is missing, the event is either ongoing or there has been manual interaction with the db
    -- that corrupted the data: to approach this, we ignore events ongoing for more than 24 hours without
    -- and ends_at timestamp
    AND 
    CASE 
      WHEN t.end_at IS NULL THEN t.start_at >= TIMESTAMP_SUB(TIMESTAMP('{{next_execution_date}}'), INTERVAL 1 DAY) 
      ELSE TRUE 
    END
),
-- deduplicate same events (location & time) of different platforms
porygon_events_dedup AS
(
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY country_code, event_type, starts_at, ends_at, ST_ASTEXT(geom) 
      ORDER BY platform
      ) AS _row_number
  FROM porygon_events_raw
),
porygon_events AS
(
  SELECT
    country_code,
    event_type,
    event_id,
    starts_at,
    ends_at,
    value,
    geo_object_type,
    geo_object_id,
    geom
  FROM porygon_events_dedup
  WHERE 
    _row_number = 1
),
outages AS
(
  SELECT 
    sd.country_code,
    sd.type AS event_type,
    CAST(sd.id AS STRING) AS event_id,
    -- conversion from local time to UTC:
    -- 1) drop timezone via DATETIME(...)  
    -- 2) use TIMESTAMP(..., tz) using the local time zone tz to cast to UTC
    TIMESTAMP(DATETIME(sd.start_at_local), c.time_zone) AS starts_at,
    TIMESTAMP(DATETIME(sd.end_at_local), c.time_zone) AS ends_at,
    1 AS value,
    -- rooster special days come per city
    'city' AS geo_object_type,
    sd.city_id AS geo_object_id,
    CAST(NULL AS GEOGRAPHY) AS geom
  FROM `fulfillment-dwh-production.dl.rooster_special_day` sd
  LEFT JOIN `fulfillment-dwh-production.dl.rooster_city` c
    ON c.country_code = sd.country_code
    AND c.id = sd.city_id
  WHERE 
    sd.type = 'outage'
  ORDER BY country_code, starts_at, ends_at
),
-- deduplicate by event type, time and location
events_raw
AS
(
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY country_code, event_type, starts_at, ends_at, geo_object_type, geo_object_id, ST_ASTEXT(geom) 
      ORDER BY event_id ASC
      ) AS _row_number
  FROM
    (
      -- TODO: there might be a more efficient than costly UNION ALL?
      SELECT 
        *,
        'porygon' AS source
      FROM porygon_events 
      WHERE 
        geom IS NOT NULL
      UNION ALL 
      SELECT 
        *,
        'pandora_legacy' AS source
      FROM pandora_legacy_events
      UNION ALL
      SELECT 
        *,
        'rooster' AS source
      FROM outages
    )
)
SELECT
  country_code,
  event_type,
  event_id,
  starts_at,
  ends_at,
  value,
  geo_object_type,
  geo_object_id,
  geom,
  source,
  source = 'pandora_legacy' AS is_pandora_legacy_event,
  DATE(starts_at) AS created_date
FROM events_raw
WHERE
  _row_number = 1
  AND TIMESTAMP_DIFF(ends_at, starts_at, DAY) <= CASE WHEN event_type = 'outage' THEN 31 ELSE 7 END
  -- remove very short events to reduce data/noise
  AND TIMESTAMP_DIFF(ends_at, starts_at, MINUTE) > 1
  -- TODO: this could be moved into subqueries for efficiency
  AND ends_at <= TIMESTAMP('{{next_execution_date}}')
