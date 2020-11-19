WITH country_weights AS (
  SELECT
    country_code,
    model_name,
    model_type,
    weight
  FROM forecasting.dataset_ts_model_weights
  WHERE country_code = '{{params.country_code}}'
)
SELECT
  COUNTIF(model_name IS NULL) = 0 AS model_name_not_null,
  COUNTIF(model_type IS NULL) = 0 AS model_type_not_null,
  COUNTIF(weight IS NULL) = 0 AS weight_not_null,
  ROUND(SUM(weight), 3) = 1 OR COUNT(*) = 0 AS weights_sum_to_1_or_table_empty
FROM country_weights
