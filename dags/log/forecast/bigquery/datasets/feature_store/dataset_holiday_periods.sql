SELECT
  d.*
FROM forecasting.dataset_holiday_periods d
WHERE 
  d.country_code = '{{params.country_code}}'
ORDER BY country_code, city_id, date
