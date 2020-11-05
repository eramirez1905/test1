SELECT
  d.*
FROM dmart_order_forecast.dataset_zone_hour d
LEFT JOIN dmart_order_forecast.zones z USING (country_code, zone_id)
WHERE 
  d.country_code = '{{params.country_code}}'
  -- only load data before midnight
  AND d.datetime < z.datetime_end_midnight
