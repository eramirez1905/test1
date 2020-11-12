WITH devices AS (
  SELECT created_date
    , assigned_at
    , updated_at
    , region
    , device_id
    , is_the_latest_version
  FROM `{{ params.project_id }}.cl.rps_devices`
), device_check_calculation AS (
  SELECT region
    , device_id
    , COUNT(*) AS _duplicate_cnt
    , COUNTIF(CONCAT(region, device_id) IS NULL) AS null_key_cnt
  FROM devices 
  WHERE is_the_latest_version
  GROUP BY 1,2
), whole_table_check_calculation AS (
  SELECT TIMESTAMP_DIFF('{{ next_execution_date }}', GREATEST(MAX(assigned_at), MAX(updated_at)), HOUR) AS time_diff
  FROM devices d
  WHERE d.is_the_latest_version
)
SELECT (COUNTIF(d._duplicate_cnt > 1) = 0) AS duplication_check
  , (SUM(d.null_key_cnt) = 0) AS null_key_check
  , (COUNTIF(w.time_diff > 24) = 0) AS timeliness_check
FROM device_check_calculation d
CROSS JOIN whole_table_check_calculation w
