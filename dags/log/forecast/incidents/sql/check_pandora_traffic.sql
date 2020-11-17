-- checking the input data sent from the Pandora DWH
-- fail if any incident type is reported as all-zero for the past 14 days
-- checking the input data sent from the Pandora DWH
-- fail if any incident type is reported as all-zero for the past 14 days
WITH incident_type_sums AS (
  SELECT
    incident_type,
    CASE WHEN SUM(count_incidents) > 0 THEN 1 ELSE 0 END AS is_available
  FROM `dl.pandora_global_cc_traffic`
  WHERE
    DATE(start_datetime) >= DATE_SUB('{{ next_ds }}', INTERVAL 14 DAY)
  GROUP BY 1
)
SELECT
  MIN(is_available) > 0 AS all_incident_types_have_recent_data
FROM incident_type_sums
