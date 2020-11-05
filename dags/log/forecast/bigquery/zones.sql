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

CREATE OR REPLACE TABLE forecasting.zones
PARTITION BY DATE(datetime_end)
CLUSTER BY country_code AS
-- check when tables were last updated using metadata.dag_executions
WITH 
tables AS (
  SELECT 
    '{{ params.dataset_cl }}' AS dataset,
    table_name
  FROM UNNEST(['countries', 'porygon_events', 'orders']) table_name
  UNION ALL
  SELECT
    '{{ params.dataset_raw }}' AS dataset,
    table_name
  FROM UNNEST(['rooster_special_day', 'rooster_city', 'forecast_adjustments']) table_name
), 
last_updates AS (
  SELECT
    t.dataset,
    t.table_name,
    MAX(next_execution_date) AS next_execution_date
  FROM tables t
  LEFT JOIN `fulfillment-dwh-production.cl.dag_executions` e USING (dataset, table_name)
  GROUP BY 1,2
)
SELECT
  c.country_code,
  z.id AS zone_id,
  z.city_id,
  z.geo_id,
  ci.timezone,
  'halal' IN UNNEST(delivery_types) AS tag_halal,
  z.distance_cap,
  z.shape AS geom,
  z.zone_shape_updated_at AS geom_updated_at,
  COALESCE(z.is_embedded, FALSE) AS is_embedded,
  z.embedding_zone_ids,
  z.area,
  -- use UTC run time (next_execution_date) to get end of local yesterday in UTC time (datetime_end_midnight)
  get_midnight_timestamp(TIMESTAMP('{{ next_execution_date }}'), ci.timezone) AS datetime_end_midnight,
  -- add local today (forecast date) which can for ex. be used by metabase
  -- dashboards instead of CURRENT_DATE() to enable caching
  DATE(get_midnight_timestamp(TIMESTAMP('{{ next_execution_date }}'), ci.timezone)) AS forecast_date,
  lu.next_execution_date AS datetime_end
FROM `fulfillment-dwh-production.cl.countries` c
LEFT JOIN UNNEST(cities) ci
LEFT JOIN UNNEST(zones) z
CROSS JOIN (SELECT MIN(next_execution_date) AS next_execution_date FROM last_updates) lu
WHERE
  z.is_active IS TRUE
  -- remove inactive but not disabled countries
  AND c.country_code NOT IN UNNEST({{ params.countries_inactive }})
