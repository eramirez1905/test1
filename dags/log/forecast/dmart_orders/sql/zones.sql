CREATE TEMPORARY FUNCTION get_midnight_timestamp(x TIMESTAMP, tz STRING) AS 
(
  -- x: time of execution
  -- DATE(x): date of execution
  -- DATETIME(DATE(x)): midnight of date of execution in local time
  -- TIMESTAMP(DATETIME(DATE(x)), tz): midnight of date of execution in UTC
  -- ex.: 
  -- 1) get_midnight_timestamp(TIMESTAMP('2019-04-16 03:00:00'), 'Europe/Berlin'):  '2019-04-15 22:00:00 UTC'
  -- 2) get_midnight_timestamp(TIMESTAMP('2019-04-16 03:00:00'), 'America/La_Paz'): '2019-04-16 04:00:00 UTC'
  -- 2) get_midnight_timestamp(TIMESTAMP('2019-04-16 07:00:00'), 'UTC'):            '2019-04-16 00:00:00 UTC'
  TIMESTAMP(DATETIME(DATE(x)), tz)
);

CREATE OR REPLACE TABLE dmart_order_forecast.zones
PARTITION BY DATE(datetime_end)
CLUSTER BY country_code 
AS
WITH
-- check when tables were last updated using metadata.dag_executions 
tables AS (
  SELECT 
    'cl' AS dataset, 
    table_name
  FROM UNNEST(['countries', 'orders']) table_name
), 
last_updates AS (
  SELECT
    t.dataset,
    t.table_name,
    MAX(next_execution_date) AS next_execution_date
  FROM tables t
  LEFT JOIN `fulfillment-dwh-production.cl.dag_executions` e USING (dataset, table_name)
  GROUP BY 1,2
),
zones AS
(
  SELECT
    c.country_code,
    z.city_id,
    z.geo_id AS zone_name,
    z.id AS zone_id,
    ci.timezone,
    z.is_active
  FROM `fulfillment-dwh-production.cl.countries` c
  LEFT JOIN UNNEST(cities) ci
  LEFT JOIN UNNEST(zones) z
  WHERE 
    STARTS_WITH(c.country_code, 'dp-')
    AND z.is_active
)
SELECT
  z.country_code,
  z.zone_id,
  z.city_id,
  -- extract fleet country code: dp-sg -> sg
  SPLIT(z.country_code, '-')[ORDINAL(2)] AS country_code_fleet,
  -- extract vendor code: dmart_vendorcode_something_else -> vendorcode
  SPLIT(z.zone_name, '_')[ORDINAL(2)] AS vendor_code,
  z.timezone,
  -- use UTC run time (next_execution_date) to get end of local yesterday in UTC time (datetime_end_midnight)
  get_midnight_timestamp(TIMESTAMP('{{ next_execution_date }}'), z.timezone) AS datetime_end_midnight,
  -- add local today (forecast date) which can for ex. be used by metabase
  -- dashboards instead of CURRENT_DATE() to enable caching
  DATE(get_midnight_timestamp(TIMESTAMP('{{ next_execution_date }}'), z.timezone)) AS forecast_date,
  lu.next_execution_date AS datetime_end
FROM zones z
CROSS JOIN (SELECT MIN(next_execution_date) AS next_execution_date FROM last_updates) lu
