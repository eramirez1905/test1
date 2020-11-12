CREATE TEMPORARY FUNCTION floor_timestamp(x TIMESTAMP) AS 
(
  TIMESTAMP_ADD(
    TIMESTAMP_TRUNC(x, HOUR), 
    INTERVAL CAST(FLOOR(EXTRACT(MINUTE FROM x) / 30) * 30 AS INT64) MINUTE
    )
);

CREATE TEMPORARY FUNCTION get_distinct_elements(x ANY TYPE) AS (
  (SELECT ARRAY_AGG(DISTINCT y) FROM UNNEST(x) AS y)
);

CREATE OR REPLACE TABLE forecasting.events_timeseries
PARTITION BY created_date
CLUSTER BY country_code, zone_id 
AS
-- aggregate to 30 minutes per value
WITH events_grid_aggregated AS
(
  SELECT
    country_code,
    --platform,
    zone_id,
    event_type,
    floor_timestamp(datetime) AS datetime,
    value,
    COUNT(*) AS n,
    ARRAY_CONCAT_AGG(event_ids) AS event_ids
  FROM forecasting.events_grid
  GROUP BY country_code, zone_id, event_type, datetime, value
),
-- compute window metrics
window_aggregates_raw AS
(
  SELECT
    *,
    FIRST_VALUE(value) OVER (
      PARTITION BY country_code, zone_id, event_type, datetime
      -- order by occurences descending, then value
      ORDER BY n DESC, value DESC
    ) AS value_median
  FROM 
  events_grid_aggregated
),
-- get window metrics per group
window_aggregates AS
(
  SELECT
    country_code,
    --platform,
    zone_id,
    event_type,
    datetime,
    ANY_VALUE(value_median) AS value_median
  FROM window_aggregates_raw
  GROUP BY country_code, zone_id, event_type, datetime
),
group_aggregates AS
(
  SELECT
    country_code,
    --platform,
    zone_id,
    event_type,
    datetime,
    -- weighted mean: (by using SUM(n) instead of 30 we can calculate the unweighted mean)
    ROUND(SUM(value * n) / 30, 2) AS value_mean,
    ROUND(SUM(value * n) / SUM(n), 2) AS value_mean_unweighted,
    MAX(value) AS value_max,
    MIN(value) AS value_min,
    SUM(n) AS duration,
    get_distinct_elements(ARRAY_CONCAT_AGG(event_ids)) AS event_ids
  FROM events_grid_aggregated
  GROUP BY country_code, zone_id, event_type, datetime
)
SELECT
  ga.country_code,
  ga.zone_id,
  ga.event_type,
  ga.datetime,
  ga.value_mean,
  ga.value_mean_unweighted,
  wa.value_median,
  ga.value_max,
  ga.value_min,
  ga.duration,
  ga.event_ids,
  DATE(ga.datetime) AS created_date
FROM group_aggregates ga
LEFT JOIN window_aggregates wa USING (country_code, zone_id, event_type, datetime)
