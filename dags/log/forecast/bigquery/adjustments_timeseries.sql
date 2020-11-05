CREATE TEMPORARY FUNCTION floor_timestamp(x TIMESTAMP) AS 
(
  TIMESTAMP_ADD(
    TIMESTAMP_TRUNC(x, HOUR), 
    INTERVAL CAST(FLOOR(EXTRACT(MINUTE FROM x) / 30) * 30 AS INT64) MINUTE
    )
);

CREATE TEMPORARY FUNCTION create_timestamp(x DATE, y TIME) AS 
(
  TIMESTAMP(DATETIME(x, y))
);

CREATE OR REPLACE TABLE forecasting.adjustments_timeseries
PARTITION BY created_date
CLUSTER BY country_code AS
WITH adjustments_raw AS 
(
  -- TODO: we could only filter for adjustments that have been done
  -- after the zone geometry has changed the last time to avoid
  -- false positives
  SELECT
    a.country_code,
    zone_id,
    a.id AS adjustment_id,
    create_timestamp(date, start_time) AS datetime_start_rooster,
    create_timestamp(date, end_time) AS datetime_end_rooster,
    date,
    start_time,
    end_time,
    a.reason,
    a.updated_at 
  FROM
    `fulfillment-dwh-production.dl.forecast_adjustments` a, 
    UNNEST(forecast_dates) AS date, 
    UNNEST(zone_ids) AS zone_id
  WHERE 
    -- only consider specifically flagged events
    reason IN ('marketing', 'special_day', 'event')
),
adjustments_fixed AS
(
  -- TODO: some start and end times are not stored properly,
  -- (ref.: https://jira.deliveryhero.com/browse/LOGR-3136)
  SELECT
    a.country_code,
    a.zone_id,
    a.adjustment_id,
    CASE 
      -- if datetime_start_rooster > datetime_end_rooster and timezone has
      -- positive offset (Europe for ex.) we need to substract a day 
      WHEN a.datetime_start_rooster > a.datetime_end_rooster AND DATETIME(a.datetime_start_rooster, z.timezone) >= DATETIME(a.datetime_start_rooster) 
        THEN TIMESTAMP_SUB(a.datetime_start_rooster, INTERVAL 1 DAY)
      ELSE a.datetime_start_rooster
    END datetime_start,
    CASE 
      -- if datetime_start_rooster > datetime_end_rooster and timezone has
      -- negative offset (South America for ex.) we need to add a day 
      WHEN a.datetime_start_rooster > a.datetime_end_rooster AND DATETIME(a.datetime_end_rooster, z.timezone) < DATETIME(a.datetime_end_rooster) 
        THEN TIMESTAMP_ADD(a.datetime_end_rooster, INTERVAL 1 DAY)
      ELSE a.datetime_end_rooster
    END datetime_end,
    a.reason,
    a.updated_at
  FROM adjustments_raw a
  LEFT JOIN forecasting.zones z USING (country_code, zone_id)
),
adjustments_unnested AS
(
  -- TODO: deduplication could be done also using 'reason' for the partitioning
  -- (similar to the 'type' in events.)
  SELECT
    *,
    -- used for deduplication
    ROW_NUMBER() OVER (PARTITION BY country_code, zone_id, datetime ORDER BY updated_at DESC) AS row_number
  FROM 
    adjustments_fixed,
    UNNEST(
      GENERATE_TIMESTAMP_ARRAY(
        datetime_start,
        datetime_end,
        INTERVAL 30 MINUTE
        )
      ) AS datetime
)
SELECT
  country_code,
  zone_id,
  datetime,
  reason,
  DATE(datetime) AS created_date
FROM adjustments_unnested
WHERE 
  row_number = 1
  AND datetime <= '{{next_execution_date}}'
