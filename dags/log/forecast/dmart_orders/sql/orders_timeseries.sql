CREATE OR REPLACE TABLE dmart_order_forecast.orders_timeseries 
PARTITION BY created_date
CLUSTER BY country_code, zone_id 
AS
SELECT 
  -- replace o.country_code (=country_code_fleet) with dmart country_code
  z.country_code,
  z.zone_id,
  o.country_code AS country_code_fleet,
  o.* EXCEPT (country_code, created_date),
  created_date
FROM `dmart_order_forecast.vendor_orders_timeseries` o
LEFT JOIN `dmart_order_forecast.zones` z
  ON z.country_code_fleet = o.country_code
  AND z.vendor_code = o.vendor_code
WHERE
  -- keep only matched time bins
  z.zone_id IS NOT NULL
  AND o.datetime <= '{{next_execution_date}}'
