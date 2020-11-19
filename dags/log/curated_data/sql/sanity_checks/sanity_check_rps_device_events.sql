WITH device_events AS (
  SELECT created_at
    , created_date
    , region
    , device_id
    , entity_id
    , vendor_code
    , status_id
  FROM `{{ params.project_id }}.cl.rps_device_events`
  -- As this table is a streaming table, we only take the latest 5 days data to be checked
  -- as past data wouldn't change when the job runs. The 5 days is to give some buffer time 
  -- when error happens.
  WHERE created_date BETWEEN DATE_SUB('{{ next_ds }}', INTERVAL 5 DAY) AND '{{ next_ds }}'
), devices AS (
  SELECT region
    , device_id
  FROM `{{ params.project_id }}.cl.rps_devices`
  GROUP BY 1,2
), device_event_check_calculation AS (
  SELECT region
    , device_id
    , vendor_code
    , entity_id
    , status_id
    , COUNT(*) AS _duplicate_cnt
    , COUNTIF(CONCAT(region, device_id, status_id) IS NULL) AS null_key_cnt
  FROM device_events 
  GROUP BY 1,2,3,4,5
), whole_table_check_calculation AS (
  SELECT COUNTIF(CONCAT(d.region, d.device_id) IS NULL) AS mismatch_vendor_cnt
    , TIMESTAMP_DIFF('{{ next_execution_date }}', MAX(created_at), HOUR) AS time_diff
  FROM device_events de
  LEFT JOIN devices d ON de.region = d.region 
    AND de.device_id = d.device_id
)
SELECT (COUNTIF(d._duplicate_cnt > 1) = 0) AS duplication_check
  , (SUM(d.null_key_cnt) = 0) AS null_key_check
  , (SUM(w.mismatch_vendor_cnt) = 0) AS refential_integrity_check
  , (COUNTIF(w.time_diff > 24) = 0) AS timeliness_check
FROM device_event_check_calculation d
CROSS JOIN whole_table_check_calculation w
