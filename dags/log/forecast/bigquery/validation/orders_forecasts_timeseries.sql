SELECT 
  COUNTIF(orders IS NULL) = 0 AS orders_not_null,
  -- TODO: this range is far from sensible but currently some staging models 
  -- procude obscure results in some zones and are not relevant for production
  COUNTIF(orders NOT BETWEEN 0 AND 1E12) = 0 AS orders_range,
  SUM(orders) > 0 AS orders_sum,
  COUNT(DISTINCT model_name) > 1 AS model_names_distinct_count, 
  COUNTIF(model_name IS NULL) = 0 AS model_name_not_null,
  SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) > 1 AS is_valid_sum,
  AVG(CASE WHEN is_valid THEN 1 ELSE 0 END) > 0.1 AS is_valid_avg,
  SUM(CASE WHEN is_most_recent THEN 1 ELSE 0 END) > 1 AS is_most_recent_sum,
  AVG(d) BETWEEN 0 AND 50 AS d_avg_range,
  COUNT(DISTINCT CONCAT(
    CAST(country_code AS STRING), 
    CAST(zone_id AS STRING), 
    CAST(datetime AS STRING),
    CAST(model_name AS STRING),
    CAST(forecast_date AS STRING)
    )) = COUNT(*) AS primary_key_condition
FROM `forecasting.orders_forecasts_timeseries` f
WHERE
    datetime <= '{{next_execution_date}}'
