WITH cities AS
(
  SELECT
    country_code,
    city_id,
    MIN(DATE(DATETIME(datetime_end_midnight, timezone))) AS date_end
  FROM forecasting.zones
  GROUP BY 1,2
)
SELECT
  d.*
FROM forecasting.dataset_city_day d
LEFT JOIN cities c USING (country_code, city_id)
WHERE 
  d.country_code = '{{params.country_code}}'
  AND d.date < c.date_end
ORDER BY country_code, city_id, date
