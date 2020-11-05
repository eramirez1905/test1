CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.rps_autocalls`
PARTITION BY created_date AS
WITH cl_countries AS (
  SELECT DISTINCT LOWER(country_iso) AS country_code
    , country_name
  FROM `{{ params.project_id }}.cl.countries`
), autocall_logs_dataset AS (
  SELECT region
    , created_date
    , status
    , answeredBy AS answered_by
    , created_at
    , UPPER(platformId) AS _platform_id
    , eventTime AS event_tme
    , platformvendorid AS vendor_code
  FROM `{{ params.project_id }}.dl.rps_phone_call_completed`
  WHERE created_date >= DATE_SUB("{{ next_ds }}", INTERVAL 29 DAY)
), autocall_logs AS(
  SELECT region
    , CASE
        WHEN SPLIT(_platform_id, '_')[SAFE_OFFSET(1)] = 'OP'
          THEN 'se'
        WHEN SPLIT(_platform_id, '_')[SAFE_OFFSET(1)] = 'PO'
          THEN 'fi'
        ELSE LOWER(SPLIT(_platform_id, '_')[SAFE_OFFSET(1)])
      END AS country_code
    , CASE
        WHEN SPLIT(_platform_id, '_')[SAFE_OFFSET(1)] = 'op' OR SPLIT(_platform_id, '_')[SAFE_OFFSET(0)] = 'op'
          THEN 'OP'
        WHEN SPLIT(_platform_id, '_')[SAFE_OFFSET(1)] = 'po' OR SPLIT(_platform_id, '_')[SAFE_OFFSET(0)] = 'po'
          THEN 'PO'
        ELSE LOWER(SPLIT(_platform_id, '_')[OFFSET(0)])
      END AS platform
    , _platform_id
    , vendor_code
    , status
    , answered_by
    , created_at
  FROM autocall_logs_dataset ca
), vendors AS (
  SELECT v.entity_id
    , v.vendor_code
    , rps.timezone
    , LOWER(rps.country_iso) AS country_code
  FROM `{{ params.project_id }}.cl.vendors_v2` v
  CROSS JOIN UNNEST(rps) rps
)
SELECT DISTINCT a.region
  , CAST(DATETIME(a.created_at, v.timezone) AS DATE) AS created_date
  , a.platform
  , c.country_name
  , a.country_code
  , a.vendor_code
  , a.status
  , a.answered_by
  , CAST(DATETIME(a.created_at, v.timezone) AS TIME) AS event_date_time
FROM autocall_logs a
LEFT JOIN vendors v ON a.vendor_code = v.vendor_code
  AND a.country_code = v.country_code
LEFT JOIN cl_countries c ON c.country_code = a.country_code
