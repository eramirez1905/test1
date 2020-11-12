CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._rps_vendor_schedules`
PARTITION BY created_date
CLUSTER BY entity_id, vendor_code AS
WITH dates_in_array AS (
  SELECT GENERATE_DATE_ARRAY(DATE_SUB(DATE_TRUNC('{{ next_ds }}', MONTH), INTERVAL 12 MONTH), '{{ next_ds }}', INTERVAL 1 DAY) AS dates_array
), dates AS (
  SELECT dates
    , EXTRACT(DAYOFWEEK FROM dates) AS day_of_week
  FROM dates_in_array
  CROSS JOIN UNNEST(dates_array) AS dates
), vendor_schedules AS (
  SELECT * EXCEPT (_rank)
  FROM (
    SELECT DATETIME(timestamp, content.time_zone) AS timestamp_local
      , content.time_zone AS timezone
      , content.vendor.id AS vendor_code
      , global_entity_id AS entity_id
      , content.schedules.monday
      , content.schedules.tuesday
      , content.schedules.wednesday
      , content.schedules.thursday
      , content.schedules.friday
      , content.schedules.saturday
      , content.schedules.sunday
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
), vendor_schedules_delivery AS (
  SELECT timestamp_local
    , next_timestamp_local
    , entity_id
    , vendor_code
    , timezone
    , 2 AS day_of_week
    , monday.from AS schedule_from_local
    , monday.to AS schedule_to_local
  FROM vendor_schedules_with_next_update
  CROSS JOIN UNNEST(monday.delivery) monday

  UNION DISTINCT

  SELECT timestamp_local
    , next_timestamp_local
    , entity_id
    , vendor_code
    , timezone
    , 3 AS day_of_week
    , tuesday.from AS schedule_from_local
    , tuesday.to AS schedule_to_local
  FROM vendor_schedules_with_next_update
  CROSS JOIN UNNEST(tuesday.delivery) tuesday

  UNION DISTINCT

  SELECT timestamp_local
    , next_timestamp_local
    , entity_id
    , vendor_code
    , timezone
    , 4 AS day_of_week
    , wednesday.from AS schedule_from_local
    , wednesday.to AS schedule_to_local
  FROM vendor_schedules_with_next_update
  CROSS JOIN UNNEST(wednesday.delivery) wednesday

  UNION DISTINCT

  SELECT timestamp_local
    , next_timestamp_local
    , entity_id
    , vendor_code
    , timezone
    , 5 AS day_of_week
    , thursday.from AS schedule_from_local
    , thursday.to AS schedule_to_local
  FROM vendor_schedules_with_next_update
  CROSS JOIN UNNEST(thursday.delivery) thursday

  UNION DISTINCT

  SELECT timestamp_local
    , next_timestamp_local
    , entity_id
    , vendor_code
    , timezone
    , 6 AS day_of_week
    , friday.from AS schedule_from_local
    , friday.to AS schedule_to_local
  FROM vendor_schedules_with_next_update
  CROSS JOIN UNNEST(friday.delivery) friday

  UNION DISTINCT

  SELECT timestamp_local
    , next_timestamp_local
    , entity_id
    , vendor_code
    , timezone
    , 7 AS day_of_week
    , saturday.from AS schedule_from_local
    , saturday.to AS schedule_to_local
  FROM vendor_schedules_with_next_update
  CROSS JOIN UNNEST(saturday.delivery) saturday

  UNION DISTINCT

  SELECT timestamp_local
    , next_timestamp_local
    , entity_id
    , vendor_code
    , timezone
    , 1 AS day_of_week
    , sunday.from AS schedule_from_local
    , sunday.to AS schedule_to_local
  FROM vendor_schedules_with_next_update
  CROSS JOIN UNNEST(sunday.delivery) sunday
), vendor_schedule_start_overlap AS (
  SELECT timestamp_local
    , next_timestamp_local
    , entity_id
    , vendor_code
    , timezone
    , day_of_week
    , schedule_from_local
    -- in case of overlapping schedules this takes the longest one
    , MAX(schedule_to_local) AS schedule_to_local
  FROM vendor_schedules_delivery
  GROUP BY 1,2,3,4,5,6,7
), vendor_schedule_end_overlap AS (
  SELECT timestamp_local
    , next_timestamp_local
    , entity_id
    , vendor_code
    , timezone
    , day_of_week
    -- in case of overlapping schedules this takes the longest one
    , MIN(schedule_from_local) AS schedule_from_local
    , schedule_to_local
  FROM vendor_schedule_start_overlap
  GROUP BY 1,2,3,4,5,6,8
), vendor_schedule_base AS (
  SELECT d.dates AS created_date_local
    , vsc.timestamp_local
    , vsc.next_timestamp_local
    , vsc.entity_id
    , vsc.vendor_code
    , vsc.timezone
    , vsc.day_of_week
    , DATETIME(
        d.dates,
        PARSE_TIME(
          '%H:%M',
          IF(schedule_from_local = '24:00', '23:59', schedule_from_local)
        )
      ) AS schedule_from_local
    , DATETIME(
        d.dates,
        PARSE_TIME(
          '%H:%M',
          IF(schedule_to_local = '24:00', '23:59', schedule_to_local)
        )
      ) AS schedule_to_local
  FROM dates d
  CROSS JOIN vendor_schedule_end_overlap vsc
  WHERE d.dates >= CAST(vsc.timestamp_local AS DATE)
    AND d.dates < CAST(vsc.next_timestamp_local AS DATE)
    AND (vsc.day_of_week IS NULL
      OR vsc.day_of_week = d.day_of_week)
), union_days_schedule AS (
  SELECT entity_id
    , vendor_code
    , day_of_week
    , created_date_local
    , schedule_from_local AS schedule_local
  FROM vendor_schedule_base

  UNION DISTINCT

  SELECT entity_id
    , vendor_code
    , day_of_week
    , created_date_local
    , schedule_to_local AS schedule_local
  FROM vendor_schedule_base
), distinct_intervals_schedule AS (
  SELECT entity_id
    , vendor_code
    , day_of_week
    , created_date_local
    , schedule_local AS schedule_from_local
    , LEAD(schedule_local) OVER (
        PARTITION BY entity_id, vendor_code
          ORDER BY schedule_local
      ) AS schedule_to_local
  FROM union_days_schedule
), deduped_intervals_schedule AS (
  SELECT DISTINCT vs.created_date_local
    , vs.timestamp_local
    , vs.next_timestamp_local
    , vs.entity_id
    , vs.vendor_code
    , vs.timezone
    , vs.day_of_week
    , di.schedule_from_local
    , di.schedule_to_local
  FROM vendor_schedule_base vs
  INNER JOIN distinct_intervals_schedule di ON vs.entity_id = di.entity_id
    AND vs.vendor_code = di.vendor_code
    AND di.schedule_from_local BETWEEN vs.schedule_from_local AND vs.schedule_to_local
    AND di.schedule_to_local BETWEEN vs.schedule_from_local AND vs.schedule_to_local
  WHERE vs.timestamp_local <= DATETIME('{{ next_execution_date }}', vs.timezone)
), vendor_schedules_full AS (
  SELECT vsb.created_date_local
    , vsb.timestamp_local
    , vsb.entity_id
    , vsb.vendor_code
    , vsb.timezone
    , vsb.day_of_week
    , vsb.schedule_from_local
    , vsb.schedule_to_local
    , vah.is_active
  FROM deduped_intervals_schedule vsb
  INNER JOIN `{{ params.project_id }}.cl._rps_vendors_is_active_history` vah ON vsb.entity_id = vah.entity_id
    AND vsb.vendor_code = vah.vendor_code
    AND vsb.schedule_from_local >= DATETIME(vah.updated_at, vsb.timezone)
    AND vsb.schedule_from_local < DATETIME(vah.next_updated_at, vsb.timezone)
  WHERE vah.is_active
), vendor_schedules_full_with_slot_duration AS (
  SELECT DISTINCT created_date_local
    , CAST(TIMESTAMP(schedule_from_local, timezone) AS DATE) AS created_date_utc
    , entity_id
    , vendor_code
    , timezone
    , schedule_from_local
    , TIMESTAMP(schedule_from_local, timezone) AS schedule_from_utc
    , schedule_to_local
    , TIMESTAMP(schedule_to_local, timezone) AS schedule_to_utc
    , is_active
    , TIMESTAMP_DIFF(
        TIMESTAMP(schedule_to_local),
        TIMESTAMP(schedule_from_local),
        SECOND
      ) AS slot_duration
    , COUNT(DISTINCT schedule_from_local) OVER (
        PARTITION BY created_date_local, entity_id, vendor_code
      ) AS slots_local
    , COUNT(DISTINCT schedule_from_local) OVER (
        PARTITION BY CAST(TIMESTAMP(schedule_from_local, timezone) AS DATE), entity_id, vendor_code
      ) AS slots_utc
  FROM vendor_schedules_full
)
SELECT DISTINCT created_date_utc AS created_date
  , entity_id
  , vendor_code
  , timezone
  , schedule_from_utc AS slot_starts_at
  , schedule_to_utc AS slot_ends_at
  , slot_duration
  , slots_utc AS slots
  , SUM(slot_duration) OVER (
      PARTITION BY created_date_utc, entity_id, vendor_code
    ) AS daily_schedule_duration
  , slots_local
  , SUM(slot_duration) OVER (
      PARTITION BY created_date_local, entity_id, vendor_code
    ) AS daily_schedule_duration_local
FROM vendor_schedules_full_with_slot_duration
