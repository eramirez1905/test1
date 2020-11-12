WITH forecasts AS (
SELECT
  c.country_code, c.city_id, w.lat, w.long, w.record_datetime, w.forecast_datetime,
  w.avg_temp_c AS temp_c, w.precipitation_mm, w.cloudy_percent, w.humidity_percent, w.feels_like_c,
  w.dew_point_c, w.wind_speed_kph, w.wind_direction_degree, w.wind_gust_kph,
  w.precipitation_prob, w.ultraviolet_index
FROM
  weather.forecast_weather w
INNER JOIN
  (SELECT country_code, city_id FROM forecasting_legacy.cities WHERE country_code = '{{params.country_code}}') c
ON
  w.city_id = c.city_id
WHERE
  data_source = '{{params.data_source}}'
AND
  record_datetime BETWEEN '{{ ds }}'::TIMESTAMP  - INTERVAL '365 days' AND '{{ ds }}'::TIMESTAMP + INTERVAL '1 day'
),
actuals AS (
SELECT
  c.country_code, c.city_id, w.record_datetime,
  w.temp_c AS temp_c_observed, w.precipitation_mm AS precipitation_mm_observed,
  w.cloudy_percent AS cloudy_percent_observed, w.humidity_percent AS humidity_percent_observed,
  w.feels_like_c AS feels_like_c_observed, w.dew_point_c AS dew_point_c_observed,
  w.wind_speed_kph AS wind_speed_kph_observed, w.wind_direction_degree AS wind_direction_degree_observed,
  w.wind_gust_kph AS wind_gust_kph_observed
FROM
  weather.historical_weather w
INNER JOIN
  (SELECT country_code, city_id FROM forecasting_legacy.cities WHERE country_code = '{{params.country_code}}') c
ON
  w.city_id = c.city_id
WHERE
  data_source = '{{params.data_source}}'
AND
  record_datetime BETWEEN '{{ ds }}'::TIMESTAMP - INTERVAL '365 days' AND '{{ ds }}'::TIMESTAMP
)
SELECT
  '{{params.data_source}}' AS data_source,
  *
FROM
 forecasts f
LEFT JOIN
  actuals a
ON
  f.country_code = a.country_code
AND
  f.city_id = a.city_id
AND
  f.forecast_datetime = a.record_datetime
