CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._rider_working_time`
PARTITION BY working_day
CLUSTER BY country_code, state, rider_id, city_id AS
WITH countries AS (
  SELECT country_code
    , country_name
    , ci.name AS city_name
    , zo.name AS zone_name
    , co.region
    , co.platforms
    , ci.id AS city_id
    , ci.timezone AS timezone
    , zo.id AS zone_id
    , sp.id AS starting_point_id
  FROM `{{ params.project_id }}.cl.countries` co
  LEFT JOIN UNNEST(co.cities) ci
  LEFT JOIN UNNEST(ci.zones) zo
  LEFT JOIN UNNEST(zo.starting_points) sp
), contracts AS (
  SELECT country_code
    , rider_id
    , (SELECT name FROM UNNEST(contracts) WHERE status = 'VALID' AND CAST(start_at AS DATE) <= '{{ next_ds }}' ORDER BY end_at DESC LIMIT 1) AS contract_name
    , (SELECT type FROM UNNEST(contracts) WHERE status = 'VALID' AND CAST(start_at AS DATE) <= '{{ next_ds }}' ORDER BY end_at DESC LIMIT 1) AS latest_contract_type
  FROM `{{ params.project_id }}.cl.riders`
), rider_transitions AS (
  SELECT *
  FROM `{{ params.project_id }}.cl._rider_transitions`
), busy_end AS (
  SELECT country_code
    , working_day
    , state
    , rider_id
    , city_id
    , starting_point_id
    , created_at_local
    , shift_started_at_local
    , shift_ended_at_local
    , timezone
  FROM (
    SELECT country_code
      , working_day
      , IF(state = 'busy_started_at', 'busy', state) AS state
      , rider_id
      , city_id
      , starting_point_id
      , created_at_local
      -- Get the latest busy_ended_at in case of multiple deliveries
      , CASE WHEN LEAD(created_at_local) OVER (PARTITION BY country_code, rider_id, shift_started_at_local, shift_ended_at_local ORDER BY created_at_local) IS NULL
            THEN TRUE
            ELSE IF(state = 'busy_ended_at' and LEAD(state) OVER (PARTITION BY country_code, rider_id, shift_started_at_local, shift_ended_at_local ORDER BY created_at_local) = 'busy_started_at',
                   TRUE,
                   FALSE
                 )
        END AS is_last
      , shift_started_at_local
      , shift_ended_at_local
      , timezone
    FROM rider_transitions
    WHERE state IN ('busy_started_at', 'busy_ended_at')
  )
  WHERE state IN ('busy_ended_at')
    AND is_last
), not_working_dataset AS (
  SELECT * FROM (
    SELECT country_code
      , working_day
      , state
      , rider_id
      , city_id
      , starting_point_id
      , created_at_local AS started_at_local
      , LEAD(created_at_local) OVER (PARTITION BY country_code, rider_id, shift_started_at_local, shift_ended_at_local ORDER BY created_at_local) AS ended_at_local
      , change_performed_by
      , change_type
      , change_reason
      , change_comment
      , timezone
    FROM rider_transitions
    WHERE state IN ('working', 'not_working', 'temp_not_working')
  )
  WHERE state IN ('temp_not_working')
), dataset AS (
  SELECT country_code
    , working_day
    , state
    , rider_id
    , city_id
    , starting_point_id
    , started_at_local
    , ended_at_local
    , change_performed_by
    , change_type
    , change_reason
    , change_comment
    , timezone
  FROM (
    SELECT country_code
      , working_day
      , state
      , rider_id
      , city_id
      , starting_point_id
      , created_at_local AS started_at_local
      , LEAD(created_at_local) OVER (PARTITION BY country_code, rider_id ORDER BY created_at_local ASC) AS ended_at_local
      , change_performed_by
      , change_type
      , change_reason
      , change_comment
      , timezone
    FROM rider_transitions
    WHERE state IN ('working', 'not_working', 'break_started_at')
    UNION ALL
    SELECT country_code
      , working_day
      , IF(state = 'busy_started_at', 'busy', state) AS state
      , rider_id
      , city_id
      , starting_point_id
      , created_at_local AS started_at_local
      , LEAD(created_at_local) OVER (PARTITION BY country_code, rider_id, shift_started_at_local, shift_ended_at_local ORDER BY created_at_local) AS ended_at_local
      , change_performed_by
      , change_type
      , change_reason
      , change_comment
      , timezone
    FROM (
      SELECT country_code
        , working_day
        , state
        , rider_id
        , city_id
        , starting_point_id
        , created_at_local
        , shift_started_at_local
        , shift_ended_at_local
        , change_performed_by
        , change_type
        , change_reason
        , change_comment
        , timezone
      FROM rider_transitions
      WHERE state IN ('busy_started_at')
      UNION ALL
      SELECT country_code
        , working_day
        , state
        , rider_id
        , city_id
        , starting_point_id
        , created_at_local
        , shift_started_at_local
        , shift_ended_at_local
        , NULL AS change_performed_by
        , NULL AS change_type
        , NULL AS change_reason
        , NULL AS change_comment
        , timezone
      FROM busy_end
    )
    UNION ALL
    SELECT country_code
      , working_day
      , IF(state = 'break_started_at', 'break', state) AS state
      , rider_id
      , city_id
      , starting_point_id
      , created_at_local AS started_at_local
      , LEAD(created_at_local) OVER (PARTITION BY country_code, rider_id, ready_id ORDER BY created_at_local) AS ended_at_local
      , change_performed_by
      , change_type
      , change_reason
      , change_comment
      , timezone
    FROM rider_transitions
    WHERE state IN ('break_started_at', 'break_ended_at')
  )
  WHERE state IN ('working', 'busy', 'break')
    AND working_day IS NOT NULL
    AND ended_at_local IS NOT NULL
  UNION ALL
  SELECT country_code
    , working_day
    , state
    , rider_id
    , city_id
    , starting_point_id
    , started_at_local
    , ended_at_local
    , change_performed_by
    , change_type
    , change_reason
    , change_comment
    , timezone
  FROM not_working_dataset
), final AS (
  -- Break down the time periods in days
  SELECT country_code
    , real_working_day as working_day
    , state
    , rider_id
    , city_id
    , starting_point_id
    , IF(real_working_day = CAST(started_at_local AS DATE),
        started_at_local,
        DATETIME(TIMESTAMP(real_working_day, timezone), timezone)
      ) AS started_at_local
    , IF(real_working_day >= CAST(ended_at_local AS DATE),
        ended_at_local,
        DATETIME(TIMESTAMP(DATE_ADD(real_working_day, INTERVAL 1 DAY), timezone), timezone)
      ) AS ended_at_local
    , change_performed_by
    , change_type
    , change_reason
    , change_comment
    , timezone
  FROM dataset
  LEFT JOIN UNNEST(GENERATE_DATE_ARRAY(CAST(started_at_local AS DATE), CAST(ended_at_local AS DATE))) real_working_day
  WHERE working_day IS NOT NULL
)
SELECT f.country_code
  , CAST(started_at_local AS DATE) AS working_day
  , co.country_name
  , co.region
  , co.city_name
  , co.zone_name
  , state
  , f.rider_id
  , f.city_id
  , f.starting_point_id
  , co.zone_id
  , started_at_local
  , ended_at_local
  , TIMESTAMP_TRUNC(TIMESTAMP(started_at_local, f.timezone), MILLISECOND) AS started_at
  , TIMESTAMP_TRUNC(TIMESTAMP(ended_at_local, f.timezone), MILLISECOND) AS ended_at
  , STRUCT(
        change_performed_by AS performed_by
      , change_type AS type
      , change_reason AS reason
      , change_comment AS comment
    ) AS details
  , c.contract_name
  , c.latest_contract_type
  , COALESCE(v.vehicle_bag, 'unknown') AS vehicle_bag
  , COALESCE(DATETIME_DIFF(ended_at_local, started_at_local, SECOND) / 60, 0) AS duration
  , f.timezone
FROM final f
LEFT JOIN contracts c USING (country_code, rider_id)
LEFT JOIN `{{ params.project_id }}.cl._vehicle_most_orders` v ON f.country_code = v.country_code
 AND CAST(f.started_at_local AS DATE) = v.report_date_local
 AND f.rider_id = v.rider_id
LEFT JOIN countries co ON f.country_code = co.country_code
  AND f.starting_point_id = co.starting_point_id
WHERE started_at_local != ended_at_local
