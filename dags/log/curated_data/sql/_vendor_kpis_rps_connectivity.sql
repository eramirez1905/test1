CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._vendor_kpis_rps_connectivity`
PARTITION BY created_date_local
CLUSTER BY entity_id, vendor_code AS
WITH rps_connectivity_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl.rps_connectivity`
), rps_connectivity AS (
  SELECT con.entity_id
    , con.vendor_code
    , DATETIME_TRUNC(slots.starts_at, HOUR) AS created_hour_local
    , slots.starts_at
    , slots.duration AS slot_duration
    , slots.daily_quantity
    , slots.connectivity
    , slots.availability
  FROM rps_connectivity_dataset con
  CROSS JOIN UNNEST(slots_local) slots
)
SELECT CAST(created_hour_local AS DATE) AS created_date_local
  , created_hour_local
  , entity_id
  , vendor_code
  , COUNT(DISTINCT starts_at) AS slots_ct
  , COUNTIF(connectivity.is_unreachable) AS unreachables_ct
  , COUNTIF(connectivity.is_unreachable AND connectivity.unreachable_duration = 0) AS unreachables_0_sec_ct
  , COUNTIF(connectivity.is_unreachable AND connectivity.unreachable_duration BETWEEN 0 AND 59) AS unreachables_60_sec_ct
  , COUNTIF(connectivity.is_unreachable AND connectivity.unreachable_duration BETWEEN 60 AND 119) AS unreachables_120_sec_ct
  , COUNTIF(connectivity.is_unreachable AND connectivity.unreachable_duration BETWEEN 120 AND 179) AS unreachables_180_sec_ct
  , COUNTIF(connectivity.is_unreachable AND connectivity.unreachable_duration > 180) AS unreachables_over_180_sec_ct
  , COUNTIF(connectivity.is_unreachable AND slot_duration = connectivity.unreachable_duration) AS unreachables_100_percent_ct
  , COALESCE(SUM(connectivity.unreachable_duration), 0) AS total_unreachable_duration
  , SUM(IF(connectivity.unreachable_duration BETWEEN 0 AND 59, connectivity.unreachable_duration, 0)) AS total_unreachable_duration_60_sec
  , SUM(IF(connectivity.unreachable_duration BETWEEN 60 AND 119, connectivity.unreachable_duration, 0)) AS total_unreachable_duration_120_sec
  , SUM(IF(connectivity.unreachable_duration BETWEEN 120 AND 180, connectivity.unreachable_duration, 0)) AS total_unreachable_duration_180_sec
  , SUM(IF(connectivity.unreachable_duration > 180, connectivity.unreachable_duration, 0)) AS total_unreachable_duration_over_180_sec
  , COUNTIF(availability.is_offline) AS offlines_ct
  , COUNTIF(availability.is_offline AND availability.offline_duration BETWEEN 0 AND 1800) AS offline_1800_sec_ct
  , COUNTIF(availability.is_offline AND availability.offline_duration > 1800) AS offline_over_1800_sec_ct
FROM rps_connectivity
GROUP BY 1,2,3,4
