SELECT
  model_name,
  model_type,
  weight
FROM forecasting.dataset_ts_model_weights
WHERE country_code = '{{params.country_code}}'
ORDER BY weight DESC
