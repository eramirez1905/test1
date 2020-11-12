INSERT INTO {{ ncr_schema }}.{{ brand_code }}_decision_engine_result_history
SELECT
  d.*,
  SYSDATE as valid_at
FROM {{ ncr_schema }}.{{ brand_code }}_decision_engine_result AS d
;
