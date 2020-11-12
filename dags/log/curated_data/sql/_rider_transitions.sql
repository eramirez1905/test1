CREATE TEMP FUNCTION get_break_time(list ARRAY<STRUCT<ready_id INT64, rider_id INT64, state STRING, created_at TIMESTAMP, order_id INT64>>)
RETURNS ARRAY<STRUCT<state STRING, created_at TIMESTAMP>>
LANGUAGE js AS """
    {% macro javascript_functions() -%}{% include "_rider_transitions.js" -%}{% endmacro -%}
    {{ javascript_functions()|indent }}
  return calculate_break_time(list);
"""
;

CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._rider_transitions`
PARTITION BY working_day
CLUSTER BY country_code, state, rider_id, created_at AS
WITH rider_transitions_raw AS (
  SELECT *
  FROM `{{ params.project_id }}.cl._rider_transitions_raw`
), rider_busy_time AS (
  SELECT *
  FROM `{{ params.project_id }}.cl._rider_busy_time`
), ready_dataset AS (
  SELECT country_code
    , state
    , courier_id
    , rider_id
    , sort_key
    , city_id
    , starting_point_id
    , vehicle_id
    , ROW_NUMBER() OVER (PARTITION BY country_code, rider_id ORDER BY created_at, sort_key) as ready_id
    , CAST(NULL AS STRING) AS change_performed_by
    , CAST(NULL AS STRING) AS change_type
    , CAST(NULL AS STRING) AS change_reason
    , CAST(NULL AS STRING) AS change_comment
    , created_at
    , DATETIME(created_at, timezone) AS created_at_local
    , shift_started_at
    , shift_ended_at
    , shift_started_at as shift_started_at_original
    , shift_ended_at as shift_ended_at_original
    , timezone
  FROM rider_transitions_raw
  WHERE state in ('ready')
), rider_transitions AS (
  SELECT country_code
    , state
    , courier_id
    , rider_id
    , sort_key
    , city_id
    , starting_point_id
    , ready_id
    , vehicle_id
    , change_performed_by
    , change_type
    , change_reason
    , change_comment
    , created_at
    , DATETIME(created_at, timezone) AS created_at_local
    -- In courier_transitions shift_started_at and shift_ended_at are not the same of the shifts in rooster.
    -- Hurrier merges shifts so here we guess what would be the shift
    , COALESCE(shift_started_at, MAX(shift_started_at) OVER (PARTITION BY country_code, rider_id, ready_id ORDER BY created_at, sort_key ROWS UNBOUNDED PRECEDING)) AS shift_started_at
    , COALESCE(shift_ended_at, MAX(shift_ended_at) OVER (PARTITION BY country_code, rider_id, ready_id ORDER BY created_at, sort_key ROWS UNBOUNDED PRECEDING)) AS shift_ended_at
    , timezone
  FROM (
    SELECT country_code
      , state
      , courier_id
      , rider_id
      , sort_key
      , city_id
      , starting_point_id
      , vehicle_id
      , COALESCE(ready_id, MAX(ready_id) OVER (PARTITION BY country_code, rider_id ORDER BY created_at, sort_key, ready_id ROWS UNBOUNDED PRECEDING)) AS ready_id
      , change_performed_by
      , change_type
      , change_reason
      , change_comment
      , created_at
      , created_at_local
      , shift_started_at
      , shift_ended_at
      , timezone
    FROM (
      SELECT country_code
        , state
        , courier_id
        , rider_id
        , sort_key
        , city_id
        , starting_point_id
        , vehicle_id
        , ready_id
        , change_performed_by
        , change_type
        , change_reason
        , change_comment
        , created_at
        , created_at_local
        , shift_started_at
        , shift_ended_at
        , timezone
      FROM ready_dataset
      UNION ALL
      SELECT country_code
        , state
        , courier_id
        , rider_id
        , sort_key
        , city_id
        , starting_point_id
        , vehicle_id
        , NULL AS ready_id
        , change_performed_by
        , change_type
        , change_reason
        , change_comment
        , created_at
        , DATETIME(created_at, timezone) AS created_at_local
        , shift_started_at
        , shift_ended_at
        , timezone
      FROM rider_transitions_raw
      WHERE state in ('working', 'break')
    )
    UNION DISTINCT
    SELECT country_code
      , state
      , courier_id
      , rider_id
      , sort_key
      , city_id
      , starting_point_id
      , vehicle_id
      , ready_id
      , change_performed_by
      , change_type
      , change_reason
      , change_comment
      , created_at
      , created_at_local
      , shift_started_at
      , shift_ended_at
      , timezone
    FROM (
      SELECT country_code
        , state
        , courier_id
        , rider_id
        , sort_key
        , city_id
        , starting_point_id
        , vehicle_id
        -- ready_id is an incremental ID used to group the shifts, an alternative of shift_id
        -- Is not possible to use shift_id because Hurrier merges shifts
        , COALESCE(ready_id, MAX(ready_id) OVER (PARTITION BY country_code, rider_id ORDER BY created_at, sort_key ROWS UNBOUNDED PRECEDING)) AS ready_id
        , change_performed_by
        , change_type
        , change_reason
        , change_comment
        , created_at
        , created_at_local
        , shift_started_at
        , shift_ended_at
        , timezone
      FROM (
        SELECT country_code
          , state
          , courier_id
          , rider_id
          , sort_key
          , city_id
          , starting_point_id
          , vehicle_id
          , ready_id
          , change_performed_by
          , change_type
          , change_reason
          , change_comment
          , created_at
          , created_at_local
          , shift_started_at
          , shift_ended_at
          , timezone
        FROM ready_dataset
        UNION ALL
        SELECT country_code
          , state
          , courier_id
          , rider_id
          , sort_key
          , city_id
          , starting_point_id
          , vehicle_id
          , NULL AS ready_id
          , change_performed_by
          , change_type
          , change_reason
          , change_comment
          , created_at
          , DATETIME(created_at, timezone) AS created_at_local
          , shift_started_at
          , shift_ended_at
          , timezone
        FROM rider_transitions_raw
        WHERE state in ('not_working', 'temp_not_working')
      )
    )
  )
), busy_time AS (
    SELECT country_code
      , state
      , rider_id
      , city_id
      , starting_point_id
      , vehicle_id
      , COALESCE(ready_id, MAX(ready_id) OVER (PARTITION BY country_code, rider_id ORDER BY created_at ROWS UNBOUNDED PRECEDING)) AS ready_id
      , created_at
      , DATETIME(created_at, timezone) AS created_at_local
      , DATETIME(shift_started_at, timezone) AS shift_started_at_local
      , DATETIME(shift_ended_at, timezone) AS shift_ended_at_local
      , COALESCE(shift_started_at, MAX(shift_started_at) OVER (PARTITION BY country_code, rider_id ORDER BY created_at ROWS UNBOUNDED PRECEDING)) AS shift_started_at
      , COALESCE(shift_ended_at, MAX(shift_started_at) OVER (PARTITION BY country_code, rider_id ORDER BY created_at ROWS UNBOUNDED PRECEDING)) AS shift_ended_at
      , timezone
      , order_id
      , delivery_id
    FROM (
      SELECT country_code
        , state
        , rider_id
        , city_id
        , starting_point_id
        , vehicle_id
        , ready_id
        , created_at
        , shift_started_at
        , shift_ended_at
        , timezone
        , NULL AS order_id
        , NULL AS delivery_id
      FROM ready_dataset
      UNION ALL
      SELECT country_code
        , state
        , rider_id
        , city_id
        , starting_point_id
        , NULL AS vehicle_id
        , NULL AS ready_id
        , created_at
        , shift_started_at
        , shift_ended_at
        , timezone
        , order_id
        , delivery_id
      FROM rider_busy_time
    )
), working_time_dataset AS (
  SELECT country_code
    , CAST(DATETIME(created_at, timezone) AS DATE) AS working_day
    , state
    , rider_id
    , city_id
    , starting_point_id
    , vehicle_id
    , ready_id
    , change_performed_by
    , change_type
    , change_reason
    , change_comment
    , created_at
    , COALESCE(shift_started_at, MAX(shift_started_at) OVER (PARTITION BY country_code, rider_id, ready_id ORDER BY created_at ROWS UNBOUNDED PRECEDING)) AS shift_started_at
    , COALESCE(shift_ended_at, MAX(shift_ended_at) OVER (PARTITION BY country_code, rider_id, ready_id ORDER BY created_at ROWS UNBOUNDED PRECEDING)) AS shift_ended_at
    , timezone
    , order_id
    , delivery_id
  FROM (
    SELECT country_code
      , state
      , rider_id
      , city_id
      , starting_point_id
      , vehicle_id
      , ready_id
      , change_performed_by
      , change_type
      , change_reason
      , change_comment
      , created_at
      , shift_started_at
      , shift_ended_at
      , timezone
      , NULL AS order_id
      , NULL AS delivery_id
    FROM rider_transitions
    UNION ALL
    SELECT country_code
      , state
      , rider_id
      , city_id
      , starting_point_id
      , vehicle_id
      , ready_id
      , NULL AS change_performed_by
      , NULL AS change_type
      , NULL AS change_reason
      , NULL AS change_comment
      , created_at
      , shift_started_at
      , shift_ended_at
      , timezone
      , order_id
      , delivery_id
    FROM busy_time
    WHERE state NOT IN ('ready')
  )
), break_time_dataset AS (
  SELECT country_code
    , working_day
    , ready_id
    , rider_id
    , break_time.state
    , break_time.created_at
    , DATETIME(break_time.created_at, timezone) AS created_at_local
    , city_id
    , starting_point_id
    , vehicle_id
    , shift_started_at
    , shift_ended_at
    , change_performed_by
    , change_type
    , change_reason
    , change_comment
    , timezone
    , order_id
    , delivery_id
  FROM (
    SELECT country_code
      , CAST(DATETIME(created_at, timezone) AS DATE) AS working_day
      , ready_id
      , rider_id
      , city_id
      , starting_point_id
      , vehicle_id
      , shift_started_at
      , shift_ended_at
      , timezone
      , created_at
      , IF(state = 'break',
          get_break_time(ARRAY_AGG(STRUCT(ready_id, rider_id, state, created_at, order_id)) OVER (PARTITION BY country_code, rider_id, ready_id ORDER BY created_at DESC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)),
          NULL
        ) AS break_time
      , state
      , NULL AS created_at_original
      , change_performed_by
      , change_type
      , change_reason
      , change_comment
      , order_id
      , delivery_id
    FROM working_time_dataset
  ) b
  LEFT JOIN UNNEST(break_time) AS break_time
  WHERE b.state = 'break'
), aggregations AS (
  SELECT country_code
    , CAST(DATETIME(created_at, timezone) AS DATE) AS working_day
    , state
    , ready_id
    , rider_id
    , city_id
    , COALESCE(starting_point_id, MAX(starting_point_id) OVER (PARTITION BY country_code, rider_id, ready_id ORDER BY created_at ROWS UNBOUNDED PRECEDING)) AS starting_point_id
    , vehicle_id
    , DATETIME(created_at, timezone) AS created_at_local
    , DATETIME(shift_started_at, timezone) AS shift_started_at_local
    , DATETIME(shift_ended_at, timezone) AS shift_ended_at_local
    , timezone
    , created_at
    , change_performed_by
    , change_type
    , change_reason
    , change_comment
    , order_id
    , delivery_id
  FROM (
    SELECT country_code
      , ready_id
      , rider_id
      , city_id
      , starting_point_id
      , vehicle_id
      , shift_started_at
      , shift_ended_at
      , timezone
      , created_at
      , state
      , change_performed_by
      , change_type
      , change_reason
      , change_comment
      , order_id
      , delivery_id
    FROM break_time_dataset
    UNION ALL
    SELECT country_code
      , ready_id
      , rider_id
      , city_id
      , IF(state IN ('ready'), starting_point_id, NULL) AS starting_point_id
      , vehicle_id
      , shift_started_at
      , shift_ended_at
      , timezone
      , created_at
      , state
      , NULL AS change_performed_by
      , NULL AS change_type
      , NULL AS change_reason
      , NULL AS change_comment
      , order_id
      , delivery_id
    FROM working_time_dataset
    WHERE state IN ('ready', 'working', 'not_working', 'temp_not_working', 'busy_started_at', 'busy_ended_at')
    UNION ALL
    SELECT country_code
      , ready_id
      , rider_id
      , city_id
      , NULL AS starting_point_id
      , CAST(NULL AS STRING) AS vehicle_id
      , shift_started_at
      , shift_ended_at
      , timezone
      -- The rule is to start working_time at the state "working" but in some cases the state "working" comes after "rider_accepted"
      , MIN(created_at) AS created_at
      , 'working' AS state
      , CAST(NULL AS STRING) AS change_performed_by
      , CAST(NULL AS STRING) AS change_type
      , CAST(NULL AS STRING) AS change_reason
      , CAST(NULL AS STRING) AS change_comment
      , MAX(order_id) AS order_id
      , MAX(delivery_id) AS delivery_id
    FROM working_time_dataset
    WHERE state IN ('working', 'busy_started_at')
    GROUP BY 1,2,3,4,5,6,7,8,9
    HAVING state = 'working'
  )
  WHERE created_at IS NOT NULL
)
SELECT country_code
  , working_day
  , state
  , ready_id
  , rider_id
  , city_id
  , starting_point_id
  , vehicle_id
  , created_at
  , created_at_local
  , shift_started_at_local
  , shift_ended_at_local
  , timezone
  , change_performed_by
  , change_type
  , change_reason
  , change_comment
  , order_id
  , delivery_id
FROM aggregations
