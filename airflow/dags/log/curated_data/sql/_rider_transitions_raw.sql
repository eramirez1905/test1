CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._rider_transitions_raw`
PARTITION BY working_day
CLUSTER BY country_code, state, rider_id, created_at AS
WITH countries AS (
  SELECT co.country_code
    , co.country_name
    , ci.id AS city_id
    , ci.name AS city_name
    , ci.timezone
    , z.id AS zone_id
    , z.name AS zone_name
    , sp.id AS starting_point_id
    , sp.name AS starting_point_name
  FROM `{{ params.project_id }}.cl.countries` co
  LEFT JOIN UNNEST(cities) ci
  LEFT JOIN UNNEST(ci.zones) z
  LEFT JOIN UNNEST(z.starting_points) sp
), riders AS (
  SELECT country_code, courier_id, rider_id
  FROM `{{ params.project_id }}.cl.riders`
  LEFT JOIN unnest(hurrier_courier_ids) courier_id
), transitions AS (
  SELECT t.country_code
    , t.created_date AS working_day
    , t.to_state AS state
    , t.courier_id
    , t.sort_key
    , c.rider_id
    , t.most_recent
    , co.timezone
    , co.city_id
    , CAST(JSON_EXTRACT_SCALAR(metadata, '$.zone_id') AS NUMERIC) AS starting_point_id
    , t.created_at
    , CAST(JSON_EXTRACT_SCALAR(metadata, '$.vehicle_id') AS STRING) AS vehicle_id
    , CAST(JSON_EXTRACT_SCALAR(metadata, '$.shift_scheduler_token') AS INT64) AS shift_id
    , CAST(JSON_EXTRACT_SCALAR(metadata, '$.shift_started_at') AS TIMESTAMP) AS shift_started_at
    , CAST(JSON_EXTRACT_SCALAR(metadata, '$.shift_ended_at') AS TIMESTAMP) AS shift_ended_at
    , CAST(JSON_EXTRACT_SCALAR(metadata, '$.performed_by') AS STRING) AS change_performed_by
    , CAST(JSON_EXTRACT_SCALAR(metadata, '$.reason') AS STRING) AS change_reason
    , CAST(JSON_EXTRACT_SCALAR(metadata, '$.comment') AS STRING) AS change_comment
    , CAST(JSON_EXTRACT_SCALAR(metadata, '$.type') AS STRING) AS change_type
  FROM `{{ params.project_id }}.dl.hurrier_courier_transitions` t
  LEFT JOIN riders c ON c.country_code = t.country_code
    AND c.courier_id = t.courier_id
  LEFT JOIN countries co ON t.country_code = co.country_code
    AND CAST(JSON_EXTRACT_SCALAR(metadata, '$.zone_id') AS NUMERIC) = co.starting_point_id
  WHERE c.rider_id IS NOT NULL
    AND t.to_state NOT IN ('late', 'available', 'starting', 'ending')
)
SELECT country_code
  , working_day
  , state
  , courier_id
  , sort_key
  , rider_id
  , IF(state IN ('ready'), shift_started_at, NULL) AS shift_started_at
  , IF(state IN ('ready'), shift_ended_at, NULL) AS shift_ended_at
  , created_at
  , DATETIME(created_at, timezone) AS created_at_local
  , vehicle_id
  , change_performed_by
  , change_type
  , change_reason
  , change_comment
  , most_recent
  , timezone
  , city_id
  , starting_point_id
FROM transitions t
