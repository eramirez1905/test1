CREATE TEMPORARY FUNCTION floor_timestamp_to_local_midnight(x TIMESTAMP, tz STRING) AS 
(
  -- x: timestamp in UTC
  -- DATETIME(x, tz): x in local time 
  -- DATE(DATETIME(x, tz))): x as local date
  -- DATETIME(DATE(DATETIME(x, tz))): x as local midnight
  -- TIMESTAMP(DATETIME(DATE(DATETIME(x, tz))), tz): x as local midnight converted back to UTC
  TIMESTAMP(DATETIME(DATE(DATETIME(x, tz))), tz)
);

CREATE OR REPLACE TABLE dmart_order_forecast.timeseries
PARTITION BY created_date
CLUSTER BY country_code, zone_id 
AS
WITH 
datetimes_start AS
(
  SELECT
    country_code,
    zone_id,
    MIN(datetime) AS datetime_start
  FROM dmart_order_forecast.orders_timeseries
  WHERE
    -- there are always some weird orders in the 1970s which would blow up the ts
    datetime >= '2018-01-01'
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
    -- TODO: replace CURRENT_TIMESTAMP with created_at of zone
    floor_timestamp_to_local_midnight(COALESCE(d.datetime_start, CURRENT_TIMESTAMP()), z.timezone) AS datetime_start,
    -- build timeseries until (but excluding) local midnight in UTC
    TIMESTAMP_SUB(z.datetime_end_midnight, INTERVAL 30 MINUTE) AS datetime_end
  -- start select on zones to always include _all_ active zones
  FROM dmart_order_forecast.zones z 
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
WHERE
  datetime <= '{{next_execution_date}}'
