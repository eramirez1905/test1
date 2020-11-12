CREATE TEMPORARY FUNCTION floor_timestamp_to_local_midnight(x TIMESTAMP, tz STRING) AS 
(
  -- x: timestamp in UTC
  -- DATETIME(x, tz): x in local time 
  -- DATE(DATETIME(x, tz))): x as local date
  -- DATETIME(DATE(DATETIME(x, tz))): x as local midnight
  -- TIMESTAMP(DATETIME(DATE(DATETIME(x, tz))), tz): x as local midnight converted back to UTC
  TIMESTAMP(DATETIME(DATE(DATETIME(x, tz))), tz)
);

CREATE OR REPLACE TABLE forecasting.timeseries
PARTITION BY created_date
CLUSTER BY country_code AS
WITH 
datetimes_start AS
(
  SELECT
    country_code,
    zone_id,
    MIN(datetime) AS datetime_start
  FROM forecasting.orders_timeseries
  WHERE
    -- there are some weird orders in the 1970s which would blow up the ts
    datetime >= '2015-12-01'
  GROUP BY 1,2
),
timeseries_start AS 
(
  SELECT
    z.country_code,
    z.zone_id,
    z.city_id,
    z.timezone,
    -- if there were no orders in the zone datetime_start is NULL, therefore take last
    -- change of zone geometry as start date of the zone
    floor_timestamp_to_local_midnight(COALESCE(d.datetime_start, z.geom_updated_at), z.timezone) AS datetime_start,
    -- build timeseries until (but excluding) local midnight in UTC
    TIMESTAMP_SUB(z.datetime_end_midnight, INTERVAL 30 MINUTE) AS datetime_end
  -- start select on zones to always include _all_ active zones
  FROM forecasting.zones z 
  LEFT JOIN datetimes_start d
    ON d.country_code = z.country_code
    AND d.zone_id = z.zone_id
),
timeseries AS
(
  SELECT
    country_code,
    zone_id,
    city_id,
    timezone,
    datetime
  FROM 
  timeseries_start,
  UNNEST(GENERATE_TIMESTAMP_ARRAY(datetime_start, datetime_end, INTERVAL 30 MINUTE)) AS datetime
)
SELECT
  country_code,
  zone_id,
  city_id,
  datetime,
  timezone,
  -- datetime_local as DATETIME, without a timezone
  DATETIME(datetime, timezone) AS datetime_local,
  DATE(datetime) AS created_date
FROM timeseries
