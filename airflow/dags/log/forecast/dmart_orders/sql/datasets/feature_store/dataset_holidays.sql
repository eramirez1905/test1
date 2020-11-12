SELECT
  d.*
FROM dmart_order_forecast.dataset_holidays d
WHERE 
  d.country_code = '{{params.country_code}}'
ORDER BY country_code, city_id, date
