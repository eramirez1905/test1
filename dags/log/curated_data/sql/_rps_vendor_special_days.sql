CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._rps_vendor_special_days`
PARTITION BY created_date
CLUSTER BY entity_id, vendor_code AS
WITH vendor_schedules AS (
  SELECT * EXCEPT (_rank)
  FROM (
    SELECT DATE(timestamp, content.time_zone) AS created_date_local
      , DATETIME(timestamp, content.time_zone) AS timestamp_local
      , timestamp
      , content.time_zone AS timezone
      , content.vendor.id AS vendor_code
      , global_entity_id AS entity_id
      , content.special_days
      -- needs to be rank due to multiple slots in schedule, this is for getting last schedule of day sent by platform
      , RANK() OVER (
          PARTITION BY global_entity_id, content.vendor.id, DATE(timestamp, content.time_zone)
            ORDER BY timestamp DESC
        ) AS _rank
    FROM `{{ params.project_id }}.dl.data_fridge_vendor_schedule_stream`
  )
  WHERE _rank = 1
), vendor_schedules_with_next_update AS (
  SELECT *
    , COALESCE(
        LEAD(timestamp_local) OVER (PARTITION BY entity_id, vendor_code ORDER BY timestamp_local),
        DATETIME('{{ next_execution_date }}', timezone)
      ) AS next_timestamp_local
  FROM vendor_schedules
), vendor_special_days AS (
  SELECT created_date_local
    , timestamp_local
    , next_timestamp_local
    , timestamp
    , entity_id
    , vendor_code
    , timezone
    , special_days.date AS special_days_date
    , delivery.reason AS delivery_reason
    , PARSE_TIME('%H:%M', IF(delivery.from = '24:00', '23:59', delivery.from)) AS delivery_from
    , PARSE_TIME('%H:%M', IF(delivery.to = '24:00', '23:59', delivery.to)) AS delivery_to
  FROM vendor_schedules_with_next_update
  CROSS JOIN UNNEST(special_days) special_days
  LEFT JOIN UNNEST(delivery) delivery
  WHERE delivery.reason IS NULL
    OR delivery.reason NOT IN ('unavailable', 'closed')
), vendor_special_days_delivery_times AS (
  SELECT created_date_local
    , timestamp_local
    , next_timestamp_local
    , timestamp
    , entity_id
    , vendor_code
    , timezone
    , special_days_date
    , delivery_reason
    -- when delivery times are null assume special day lasted the whole day
    -- later on in the query these cases will have their duration set to 0
    , DATETIME(special_days_date, COALESCE(delivery_from, '00:00:00')) AS delivery_from_local
    , DATETIME(special_days_date, COALESCE(delivery_to, '23:59:59')) AS delivery_to_local
  FROM vendor_special_days
  WHERE DATETIME(special_days_date, COALESCE(delivery_from, '00:00:00')) <= DATETIME('{{ next_execution_date }}', timezone)
), vendor_special_days_start_overlap AS (
  SELECT created_date_local
    , timestamp_local
    , next_timestamp_local
    , timestamp
    , entity_id
    , vendor_code
    , timezone
    , special_days_date
    , delivery_reason
    -- in case of overlapping schedules this takes the longest one
    , delivery_from_local
    , MAX(delivery_to_local) AS delivery_to_local
  FROM vendor_special_days_delivery_times
  GROUP BY 1,2,3,4,5,6,7,8,9,10
), vendor_schedule_end_overlap AS (
  SELECT created_date_local
    , timestamp_local
    , next_timestamp_local
    , timestamp
    , entity_id
    , vendor_code
    , timezone
    , special_days_date
    , delivery_reason
    -- in case of overlapping schedules this takes the longest one
    , MIN(delivery_from_local) AS delivery_from_local
    , delivery_to_local
  FROM vendor_special_days_start_overlap
  GROUP BY 1,2,3,4,5,6,7,8,9,11
), vendor_special_days_previous_days_update AS (
  SELECT *
  FROM (
    SELECT *
      , ROW_NUMBER() OVER (
          PARTITION BY entity_id, vendor_code, special_days_date
            ORDER BY timestamp_local DESC
        ) AS _row_number
    FROM vendor_schedule_end_overlap
    -- remove schedule updates after the special day due date or in its due date
    WHERE created_date_local < special_days_date
  )
 -- filter the last schedule update for each special day
  WHERE (_row_number = 1)
), vendor_special_days_previous_days_update_valid AS (
  SELECT *
  FROM vendor_special_days_previous_days_update
  -- remove special days that were removed from schedules before its due date
  WHERE (delivery_from_local < next_timestamp_local)
), vendor_special_days_same_day_update AS (
  SELECT *
  FROM vendor_schedule_end_overlap
  -- filter only special days with schedule updates in its due date
  WHERE created_date_local = special_days_date
), vendor_special_days_same_day_last_update_time AS (
  SELECT *
  FROM (
    SELECT *
      , ROW_NUMBER() OVER (
          PARTITION BY entity_id, vendor_code, special_days_date
            ORDER BY timestamp_local DESC
        ) AS _row_number
    FROM vendor_special_days_same_day_update
  )
  -- filter the latest special day update in the day
  WHERE _row_number = 1
), vendor_special_days_updates_combined AS (
  SELECT entity_id
    , vendor_code
    , timezone
    , special_days_date
    , delivery_reason
    , created_date_local
    , timestamp_local
    , timestamp
    , delivery_from_local
    , delivery_to_local
  FROM vendor_special_days_previous_days_update_valid

  UNION DISTINCT

  SELECT entity_id
    , vendor_code
    , timezone
    , special_days_date
    , delivery_reason
    , created_date_local
    , timestamp_local
    , timestamp
    , delivery_from_local
    , delivery_to_local
  FROM vendor_special_days_same_day_last_update_time
), vendor_special_days_updates_combined_cleaned AS (
  SELECT * EXCEPT (_row_number)
  FROM (
    SELECT *
      , ROW_NUMBER() OVER (
          PARTITION BY entity_id, vendor_code, special_days_date, delivery_from_local, delivery_to_local
          ORDER BY timestamp_local
        ) AS _row_number
    FROM vendor_special_days_updates_combined
  )
  -- remove cases when schedule was updated but special day times did not change in its due date
  WHERE _row_number = 1
), vendor_special_days_updates_combined_cleaned_latest_change AS (
  SELECT *
  FROM (
    SELECT *
      , ROW_NUMBER() OVER (
          PARTITION BY entity_id, vendor_code, special_days_date
          ORDER BY timestamp_local DESC
        ) AS _row_number
    FROM vendor_special_days_updates_combined_cleaned
  )
  -- filter latest special day times change
  WHERE _row_number = 1
), vendor_special_days_greatest AS (
SELECT created_date_local
  , sd.entity_id
  , sd.vendor_code
  , sd.timezone
  , sd.timestamp
  , sd.delivery_reason
  -- special day starts in the delivery_from timestamp unless it was created after that
  , GREATEST(sd.delivery_from_local, DATETIME(sd.timestamp, sd.timezone)) AS delivery_from_local
  , sd.delivery_to_local
FROM vendor_special_days_updates_combined_cleaned_latest_change sd
LEFT JOIN `{{ params.project_id }}.cl._rps_vendors_is_active_history` vah ON sd.entity_id = vah.entity_id
  AND sd.vendor_code = vah.vendor_code
  AND sd.delivery_from_local >= DATETIME(vah.updated_at, sd.timezone)
  AND sd.delivery_from_local < DATETIME(vah.next_updated_at, sd.timezone)
WHERE vah.is_active
), vendor_special_days_adjusted AS (
SELECT created_date_local
  , entity_id
  , vendor_code
  , timezone
  , timestamp AS updated_at
  , delivery_reason
  , delivery_from_local
  -- case the vendor was closed the whole day due to schedule day, the end time is set as the start time so special day duration equals 0
  -- the only way to get 23:59:59 timestamp is if the restaurant as closed for the whole day, manually set in vendor_special_days_delivery_times CTE
  , IF(delivery_to_local = DATETIME(CAST(delivery_to_local AS DATE), '23:59:59'),
      delivery_from_local,
      delivery_to_local
    ) AS delivery_to_local
FROM vendor_special_days_greatest
), vendor_special_days_duration AS (
  SELECT *
    , GREATEST(
          TIMESTAMP_DIFF(
            TIMESTAMP(delivery_to_local, timezone),
            TIMESTAMP(delivery_from_local, timezone),
            SECOND),
          0
      ) AS special_day_duration
    , TIMESTAMP(delivery_from_local, timezone) AS delivery_from_utc
    , TIMESTAMP(delivery_to_local, timezone) AS delivery_to_utc
  FROM vendor_special_days_adjusted
)
SELECT CAST(delivery_from_utc AS DATE) AS created_date
  , entity_id
  , vendor_code
  , timezone
  , updated_at
  , delivery_reason
  , delivery_from_utc AS special_day_starts_at
  , delivery_to_utc AS special_day_ends_at
  , special_day_duration
  , SUM(special_day_duration) OVER (
      PARTITION BY CAST(delivery_from_utc AS DATE), entity_id, vendor_code
    ) AS daily_special_day_duration
  , SUM(special_day_duration) OVER (
      PARTITION BY created_date_local, entity_id, vendor_code
    ) AS daily_special_day_duration_local
FROM vendor_special_days_duration
