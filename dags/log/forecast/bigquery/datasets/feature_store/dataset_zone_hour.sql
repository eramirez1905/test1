SELECT
  d.*
FROM forecasting.dataset_zone_hour d
LEFT JOIN forecasting.zones z USING (country_code, zone_id)
WHERE 
  d.country_code = '{{params.country_code}}'
  -- only load data before midnight
  AND d.datetime < z.datetime_end_midnight
