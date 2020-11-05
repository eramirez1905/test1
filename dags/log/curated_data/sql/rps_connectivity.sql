CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.rps_connectivity`
CLUSTER BY entity_id, vendor_code AS
WITH rps_monitor_data AS (
  SELECT * EXCEPT (_row_number)
  FROM (
    SELECT region
      , CASE
          WHEN LOWER(platformId) = 'foodora' AND SUBSTR(platformRestaurantId, 0, 2) IN ('AT','CA','FI','NO','SE')
            THEN CONCAT('FO_', SUBSTR(platformRestaurantId, 0, 2))
          WHEN LOWER(platformId) = 'foodora' AND SUBSTR(platformRestaurantId, 0, 2) IN ('MY','PH','PK','HK','RO','SG','TH','TW','BD','BG','LA','MM','KH')
            THEN CONCAT('FP_', SUBSTR(platformRestaurantId, 0, 2))
          WHEN SUBSTR(platformRestaurantId, 0, 2) = 'PO'
            THEN 'PO_FI'
          WHEN SUBSTR(platformRestaurantId, 0, 2) = 'OP'
            THEN 'OP_SE'
          ELSE UPPER(platformId)
        END AS entity_id
      , LOWER(platformId) as _legacy_platform_id
      , eventTime AS event_time
      , IF(LOWER(platformId) = 'foodora',
          SPLIT(platformRestaurantId, '-')[OFFSET(1)],
          platformRestaurantId
        ) AS _platform_restaurant_id
      , UPPER(platformId) AS platform_id
      , event
      , isOpen AS is_open
      , isOnline AS is_online
      , closingReason AS closing_reason
      , ClosedByUsAt AS closed_by_us_at
      , openType AS open_type
      , created_date
      , ROW_NUMBER() OVER (PARTITION BY LOWER(platformId), platformRestaurantId, event, TIMESTAMP_TRUNC(eventTime, MINUTE)
          ORDER BY eventTime DESC
        ) AS _row_number
    FROM `{{ params.project_id }}.dl.rps_monitor`
    WHERE created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 12 MONTH)
  )
  WHERE _row_number = 1
), vendor_mapping AS (
  SELECT region
    , entity_id
    , platform_id
    , _legacy_platform_id
    , _platform_restaurant_id
  FROM rps_monitor_data
  GROUP BY 1,2,3,4,5
), restaurant_2_delivery_platform AS (
  SELECT rdp.belongs_to AS vendor_id
    , rdp.external_id AS vendor_code
    , rdp.region
    , dp.global_key AS rps_global_key
    , rdp.platform_info
  FROM `{{ params.project_id }}.dl.icash_restaurant_2_delivery_platform` rdp
  LEFT JOIN `{{ params.project_id }}.dl.icash_delivery_platform` dp ON rdp.delivery_platform = dp.id
    AND rdp.region = dp.region
), vendors AS (
  SELECT *
  FROM (
    SELECT v.entity_id
      , v.vendor_code
      , IF(
          SPLIT(rps.rps_global_key, '_')[OFFSET(0)] IN ('FP', 'FO'),
          JSON_EXTRACT(rdp.platform_info, "$['vendor']"),
          v.vendor_code
        ) AS _platform_restaurant_id
      , v.is_monitor_enabled
      , rps.region
      , rps.vendor_id
      , rps.operator_code AS _operator_code
      , LOWER(rps.country_iso) AS country_code
      , rps.rps_global_key
      , rps.timezone
      , ROW_NUMBER() OVER (PARTITION BY v.entity_id, v.vendor_code ORDER BY rps.updated_at DESC) AS _row_number
    FROM `{{ params.project_id }}.cl.vendors_v2` v
    LEFT JOIN UNNEST(rps) rps
    LEFT JOIN restaurant_2_delivery_platform rdp ON rps.vendor_id = rdp.vendor_id
      AND v.vendor_code = rdp.vendor_code
      AND rps.region = rdp.region
  )
  WHERE _row_number = 1
), vendor_monitor_mapped AS (
  SELECT m.region
    , v.rps_global_key
    , v.country_code
    , v.entity_id
    , IF(m._legacy_platform_id = 'foodora', v._platform_restaurant_id, v.vendor_code) AS _platform_restaurant_id
    , v.vendor_code
    , m.platform_id
    , m._legacy_platform_id
    , v.timezone
    , v._operator_code
    , v.vendor_id
    , v.is_monitor_enabled
  FROM vendor_mapping m
  -- Supporting mapping monitor aligning mapping to RPS standards - Feb 2020
  INNER JOIN vendors v ON m.entity_id = v.rps_global_key
    AND m._platform_restaurant_id = IF(m._legacy_platform_id = 'foodora', v._platform_restaurant_id, v.vendor_code)
), rps_monitor_data_processed AS (
  SELECT m.region
    , v.rps_global_key
    , v.country_code
    , v.entity_id
    , m.event_time
    , m.created_date
    , v.vendor_code
    , m.platform_id
    , v.timezone
    , v._operator_code
    , v.vendor_id
    , v.is_monitor_enabled
    , m.event
    , m.is_open
    , m.is_online
    , m.closing_reason
    , m.closed_by_us_at
    , m.open_type
    , LAST_VALUE(m.event) OVER (
        platform_vendor_window ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ) AS vendor_last_event
    , LAST_VALUE(m.closing_reason) OVER (
        platform_vendor_window ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ) AS vendor_last_closing_reason
    , LAST_VALUE(m.is_open) OVER (
        platform_vendor_window ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ) AS vendor_last_is_open
    , FIRST_VALUE(IF(m.event = 'online', m.event_time , NULL) IGNORE NULLS) OVER (
        platform_vendor_window ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
      ) AS vendor_next_online_time
    , LAST_VALUE(IF(m.event = 'online', m.event_time, NULL) IGNORE NULLS) OVER (
        platform_vendor_window ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS vendor_last_online_time
    , LAST_VALUE(IF(m.event = 'offline', m.event_time, NULL) IGNORE NULLS) OVER (
        platform_vendor_window ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS vendor_last_offline_time
    , FIRST_VALUE(IF(m.event = 'opened', m.event_time , NULL) IGNORE NULLS) OVER (
        platform_vendor_window ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
      ) AS vendor_next_opened_time
    , FIRST_VALUE(
        IF(m.event = 'closed' AND m.closing_reason NOT IN ('UNREACHABLE_OPEN', 'UNREACHABLE'),
          m.event_time,
          NULL
        ) IGNORE NULLS) OVER (platform_vendor_window ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
      ) AS vendor_next_closed_time
    , COALESCE(
        (m.event = 'opened' AND NOT m.is_online) OR (m.event = 'offline' AND m.is_open AND NOT m.is_online),
        FALSE
      ) AS is_unreachable
    , IF(
        (m.event = 'opened' AND NOT m.is_online) OR (m.event = 'offline' AND m.is_open AND NOT m.is_online),
        m.event_time,
        NULL
      ) AS unreachable_starts_at
    , COALESCE(
        (m.event = 'closed' AND m.closing_reason IN ('UNREACHABLE_OPEN', 'UNREACHABLE') AND NOT m.is_open AND NOT m.is_online),
        FALSE
      ) AS is_offline
    , IF(
        m.event = 'closed' AND m.closing_reason IN ('UNREACHABLE_OPEN', 'UNREACHABLE') AND NOT m.is_open AND NOT m.is_online,
        m.closed_by_us_at,
        NULL
      ) AS offline_starts_at
    , IF(
        m.event = 'closed' AND NOT m.is_open AND NOT m.is_online,
        m.closing_reason,
        NULL
      ) AS offline_reason
    , COUNTIF(m.event IN ('online','offline')) OVER (
        platform_vendor_window
      ) AS online_offline_events
    , COUNTIF(m.event IN ('opened','closed')) OVER (
        platform_vendor_window
      ) AS opened_closed_events
    , MAX(
        IF(
          m.event IN ('opened','closed'),
          m.event_time,
          NULL
        )) OVER (platform_vendor_window
      ) AS last_schedule_update_time
    , MAX(
        IF(
          m.event IN ('online','offline'),
          m.event_time,
          NULL)) OVER (platform_vendor_window
      ) AS last_status_update_time
    , COUNTIF(m.event IN ('opened') AND m.open_type = 'weeklySchedule') OVER (
        platform_vendor_date_window
      ) AS opened_events_day
    , COUNTIF(m.event IN ('closed') AND m.open_type = 'weeklySchedule') OVER (
        platform_vendor_date_window
      ) AS closed_events_day
  FROM rps_monitor_data m
  -- Supporting mapping for Vendor Monitor hard switching for the values in `platformRestaurantId` for Pandora Vendors.
  -- Values before applies the format of {country_code} + {vendor_id}, and it changes to just `vendor_code` after the switch.
  INNER JOIN vendor_monitor_mapped v ON m.entity_id = v.rps_global_key
    AND m._legacy_platform_id = v._legacy_platform_id
    AND m._platform_restaurant_id = v._platform_restaurant_id
  WINDOW platform_vendor_window AS (
    PARTITION BY m.entity_id, v.vendor_code
      ORDER BY event_time
  ), platform_vendor_date_window AS (
    PARTITION BY m.entity_id, v.vendor_code, CAST(m.event_time AS DATE)
      ORDER BY event_time
  )
), rps_monitor_data_clean AS (
  SELECT rps_global_key
    , country_code
    , entity_id
    , event_time
    , created_date
    , vendor_code
    , platform_id
    , timezone
    , _operator_code
    , vendor_id
    , is_monitor_enabled
    , event
    , is_open
    , is_online
    , closing_reason
    , closed_by_us_at
    , open_type
    , vendor_next_online_time
    , vendor_last_online_time
    , vendor_last_offline_time
    , vendor_next_closed_time
    , vendor_next_opened_time
    , is_unreachable
    , unreachable_starts_at
    , FIRST_VALUE(unreachable_starts_at IGNORE NULLS) OVER (
          platform_vendor_window ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
      ) AS next_unreachable_starts_at
    , is_offline
    , offline_starts_at
    , offline_reason
    , online_offline_events
    , opened_closed_events
    , last_schedule_update_time
    , last_status_update_time
    , opened_events_day
    , closed_events_day
  FROM rps_monitor_data_processed
  WHERE event <> vendor_last_event
    AND NOT (is_unreachable AND vendor_last_event = 'closed' AND vendor_last_closing_reason IN ('UNREACHABLE_OPEN', 'UNREACHABLE'))
    AND NOT (is_unreachable AND event = 'opened' AND vendor_last_event = 'offline' AND vendor_last_closing_reason IN ('UNREACHABLE_OPEN', 'UNREACHABLE'))
    AND NOT (is_unreachable AND event = 'opened' AND vendor_last_is_open)
  WINDOW platform_vendor_window AS (
    PARTITION BY entity_id, vendor_code
      ORDER BY event_time
  )
), rps_monitor_data_with_durations AS (
  SELECT *
    , TIMESTAMP_DIFF(
        IF(
          vendor_next_opened_time < vendor_next_closed_time,
          vendor_next_opened_time,
          vendor_next_closed_time
        ),
        offline_starts_at,
        SECOND
      ) AS offline_duration
    -- unreachable duration ends when either the vendor gets back online or there's a new unreachable event
    -- case there's no next event the duration is calculated based on current time, this is fixed in the next CTE
    , TIMESTAMP_DIFF(
        COALESCE(
          LEAST(
            COALESCE(next_unreachable_starts_at, vendor_next_online_time),
            COALESCE(vendor_next_online_time, next_unreachable_starts_at)
          ),
          '{{ next_execution_date }}'
        ),
        unreachable_starts_at,
        SECOND
      ) AS unreachable_duration
  FROM rps_monitor_data_clean
), valid_connectivity_entities AS (
 -- this CTE is meant to remove schedules from vendors that haven't received any
 -- vendor monitor unreachable or offline event
 SELECT entity_id
    , MIN(event_time) AS first_event_time
    , MAX(event_time) AS last_event_time
 FROM rps_monitor_data_processed
 WHERE (is_unreachable or is_offline)
 GROUP BY 1
), schedules AS (
  SELECT v.region
    , v.country_code
    , sc.entity_id
    , v.rps_global_key
    , v.vendor_code
    , v._operator_code
    , v.vendor_id
    , v.is_monitor_enabled
    , sc.timezone
    , sc.slot_starts_at
    , sc.slot_ends_at
    , sc.slot_duration
    , sc.slots
    , sc.daily_schedule_duration
    , sc.slots_local
    , sc.daily_schedule_duration_local
  FROM `{{ params.project_id }}.cl._rps_vendor_schedules` sc
  INNER JOIN vendors v ON sc.entity_id = v.entity_id
    AND sc.vendor_code = v.vendor_code
  INNER JOIN valid_connectivity_entities vce ON sc.entity_id = vce.entity_id
    AND CAST(sc.slot_starts_at AS DATE) > CAST(vce.first_event_time AS DATE)
    AND CAST(sc.slot_starts_at AS DATE) <= CAST(vce.last_event_time AS DATE)
), rps_schedules_with_monitor_data AS (
    SELECT CAST(s.slot_starts_at AS DATE) AS created_date
      , s.region
      , s.rps_global_key
      , s.country_code
      , s.entity_id
      , s.timezone
      , s.vendor_code
      , s._operator_code
      , s.vendor_id
      , s.is_monitor_enabled
      , s.slot_starts_at
      , s.slot_ends_at
      , s.slot_duration
      , s.daily_schedule_duration
      , s.daily_schedule_duration_local
      , s.slots
      , s.slots_local
      , mdd.is_unreachable
      , mdd.unreachable_starts_at
      , mdd.next_unreachable_starts_at
      -- unreachable end timestamp is limited by slot end
      , IF(
          TIMESTAMP_ADD(mdd.unreachable_starts_at, INTERVAL mdd.unreachable_duration SECOND) >= s.slot_ends_at,
          s.slot_ends_at,
          TIMESTAMP_ADD(mdd.unreachable_starts_at, INTERVAL mdd.unreachable_duration SECOND)
        ) AS unreachable_ends_at
      , mdd.is_offline
      , mdd.offline_reason
      , mdd.offline_starts_at
      -- offline end timestamp is limited by slot end
      , IF(
          TIMESTAMP_ADD(mdd.offline_starts_at, INTERVAL mdd.offline_duration SECOND) >= s.slot_ends_at,
          s.slot_ends_at,
          TIMESTAMP_ADD(mdd.offline_starts_at, INTERVAL mdd.offline_duration SECOND)
        ) AS offline_ends_at
      , mdd.vendor_next_online_time
      , mdd.vendor_last_online_time
      , mdd.vendor_last_offline_time
      , mdd.event_time
      , MD5(
          CONCAT(
            s.entity_id,
            s.vendor_code,
            COALESCE(CAST(mdd.vendor_next_online_time AS STRING), ''),
            COALESCE(CAST(mdd.vendor_last_online_time AS STRING), ''),
            COALESCE(CAST(mdd.vendor_last_offline_time AS STRING), ''),
            COALESCE(CAST(mdd.vendor_next_opened_time AS STRING), ''),
            CAST(s.slot_starts_at AS STRING),
            CAST(s.slot_ends_at AS STRING)
          )
        ) AS unreachable_id
    FROM schedules s
    LEFT JOIN rps_monitor_data_with_durations mdd ON s.entity_id = mdd.entity_id
      AND s.vendor_code = mdd.vendor_code
      -- unreachable and offline can start out of slot hours as long they happen on same local schedule day
      AND (
        DATE(s.slot_starts_at, s.timezone) = DATE(mdd.unreachable_starts_at, s.timezone)
          OR DATE(s.slot_starts_at, s.timezone) = DATE(mdd.offline_starts_at, s.timezone)
      )
    WHERE s.slot_starts_at IS NOT NULL
), rps_schedules_with_monitor_data_clean AS (
  SELECT created_date
      , region
      , rps_global_key
      , country_code
      , entity_id
      , timezone
      , vendor_code
      , _operator_code
      , vendor_id
      , is_monitor_enabled
      , slot_starts_at
      , slot_ends_at
      , slot_duration
      , daily_schedule_duration
      , daily_schedule_duration_local
      , slots
      , slots_local
      -- this attributes unreachable and offline events to their respective slot
      , (unreachable_starts_at < slot_ends_at AND unreachable_ends_at > slot_starts_at) AS is_unreachable
      , IF(
          unreachable_starts_at < slot_ends_at AND unreachable_ends_at > slot_starts_at,
          unreachable_starts_at,
          NULL
        ) AS unreachable_starts_at
      , IF(
          unreachable_starts_at < slot_ends_at AND unreachable_ends_at > slot_starts_at,
          unreachable_ends_at,
          NULL
        ) AS unreachable_ends_at
      , (offline_starts_at < slot_ends_at AND offline_ends_at > slot_starts_at) AS is_offline
      , IF(
          offline_starts_at < slot_ends_at AND offline_ends_at > slot_starts_at,
          offline_reason,
          NULL
        ) AS offline_reason
      , IF(
          offline_starts_at < slot_ends_at AND offline_ends_at > slot_starts_at,
          offline_starts_at,
          NULL
        ) AS offline_starts_at
      , IF(
          offline_starts_at < slot_ends_at AND offline_ends_at > slot_starts_at,
          offline_ends_at,
          NULL
        ) AS offline_ends_at
      , vendor_next_online_time
      , vendor_last_online_time
      , vendor_last_offline_time
      , event_time
      , unreachable_id
  FROM rps_schedules_with_monitor_data
), rps_schedules_with_unreachable_timestamps AS (
  SELECT *
    -- get the time where first unreachable event started in vendor the slot
    , FIRST_VALUE(unreachable_starts_at IGNORE NULLS) OVER (
        platform_vendor_window ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS first_unreachable_starts_at
    -- get the time where last unreachable event started
    , LAST_VALUE(unreachable_starts_at IGNORE NULLS) OVER (
        PARTITION BY entity_id, vendor_code ORDER BY slot_starts_at
      ) AS last_unreachable_starts_at
  FROM rps_schedules_with_monitor_data_clean
  WINDOW platform_vendor_window AS (
    PARTITION BY entity_id, vendor_code, slot_starts_at
      ORDER BY event_time
  )
), rps_schedules_with_monitor_prev_unreachable AS (
  SELECT *
    -- get the previous unreachable event start time
    , LAG(unreachable_starts_at, 1) OVER (
        PARTITION BY entity_id, vendor_code ORDER BY last_unreachable_starts_at
      ) AS previous_unreachable_starts_at
  FROM rps_schedules_with_unreachable_timestamps
), rps_schedules_with_monitor_adjusted_unreachable_start AS (
  SELECT *
    -- check for vendors that were unreachable since the slot start
    , IF(
        unreachable_starts_at = first_unreachable_starts_at
          AND (previous_unreachable_starts_at > vendor_last_online_time OR vendor_last_online_time IS NULL),
        slot_starts_at,
        unreachable_starts_at
      ) AS adjusted_unreachable_starts_at
  FROM rps_schedules_with_monitor_prev_unreachable
), rps_schedules_with_monitor_limited_starts AS (
  SELECT *
    -- limit unreachable start by slot start
    , GREATEST(adjusted_unreachable_starts_at, slot_starts_at) AS limited_unreachable_starts_at
    -- limit offline start by slot start
    , GREATEST(offline_starts_at, slot_starts_at) AS limited_offline_starts_at
  FROM rps_schedules_with_monitor_adjusted_unreachable_start
), rps_monitor_connectivity_data AS (
  SELECT DISTINCT created_date
    , region
    , country_code
    , entity_id
    , vendor_code
    , timezone
    , _operator_code
    , vendor_id
    , is_monitor_enabled
    , event_time
    , slot_starts_at
    , slot_ends_at
    , slot_duration
    , daily_schedule_duration
    , daily_schedule_duration_local
    , slots
    , slots_local
    , unreachable_id
    , COALESCE(MAX(is_unreachable) OVER (unreachable_event_window), FALSE) AS is_unreachable
    , MAX(limited_unreachable_starts_at) OVER (unreachable_event_window) AS unreachable_starts_at
    , MAX(unreachable_ends_at) OVER (unreachable_event_window) AS unreachable_ends_at
    , COALESCE(MAX(is_offline) OVER (unreachable_event_window), FALSE) AS is_offline
    , MAX(offline_reason) OVER (unreachable_event_window) AS offline_reason
    , MAX(limited_offline_starts_at) OVER (unreachable_event_window) AS offline_starts_at
    , MAX(offline_ends_at) OVER (unreachable_event_window) AS offline_ends_at
  FROM rps_schedules_with_monitor_limited_starts
  WINDOW unreachable_event_window AS (
    PARTITION BY entity_id, vendor_code, unreachable_id
  )
), rps_monitor_connectivity_data_deduplicated AS (
  SELECT *
  FROM (
    SELECT *
      , ROW_NUMBER() OVER (
          PARTITION BY created_date, entity_id, vendor_code, slot_starts_at, unreachable_starts_at, offline_starts_at
            ORDER BY unreachable_starts_at
        ) AS _row_number
    FROM rps_monitor_connectivity_data
  )
  WHERE _row_number = 1
), rps_monitor_connectivity_adjusted_to_special_days AS (
  SELECT mcd.created_date
    , mcd.region
    , mcd.country_code
    , mcd.entity_id
    , mcd.vendor_code
    , mcd.timezone
    , mcd._operator_code
    , mcd.vendor_id
    , mcd.is_monitor_enabled
    , mcd.slot_starts_at
    , mcd.slot_ends_at
    , mcd.slot_duration
    , mcd.daily_schedule_duration
    , mcd.daily_schedule_duration_local
    , mcd.slots
    , mcd.slots_local
    , sd.special_day_starts_at
    , sd.special_day_ends_at
    , sd.special_day_duration
    , sd.daily_special_day_duration
    , sd.daily_special_day_duration_local
    , mcd.is_unreachable
    , mcd.unreachable_starts_at
    -- in case of special day, only considers unreachable events that started within special day duration
    , IF(
        (mcd.unreachable_starts_at BETWEEN sd.special_day_starts_at AND sd.special_day_ends_at)
          OR (sd.special_day_starts_at IS NULL),
        mcd.unreachable_starts_at,
        NULL
      ) AS unreachable_starts_at_adjusted
    , mcd.unreachable_ends_at
    -- limit unreachability by special day end or slot end
    , IF(
        (mcd.unreachable_starts_at BETWEEN sd.special_day_starts_at AND sd.special_day_ends_at)
          OR (mcd.unreachable_starts_at IS NOT NULL AND sd.special_day_starts_at IS NULL),
        LEAST(
          COALESCE(mcd.unreachable_ends_at, '{{ next_execution_date }}'),
          COALESCE(sd.special_day_ends_at, '{{ next_execution_date }}'),
          mcd.slot_ends_at
        ),
        NULL
      ) AS unreachable_ends_at_adjusted
    , mcd.is_offline
    , mcd.offline_reason
    , mcd.offline_starts_at
    , IF(
        (mcd.offline_starts_at BETWEEN sd.special_day_starts_at AND sd.special_day_ends_at)
          OR (sd.special_day_starts_at IS NULL),
        mcd.offline_starts_at,
        NULL
      ) AS offline_starts_at_adjusted
    , mcd.offline_ends_at
    , IF(
        (mcd.offline_starts_at BETWEEN sd.special_day_starts_at AND sd.special_day_ends_at)
          OR (mcd.offline_starts_at IS NOT NULL AND sd.special_day_starts_at IS NULL),
        LEAST(COALESCE(mcd.offline_ends_at, '{{ next_execution_date }}'),
              COALESCE(sd.special_day_ends_at, '{{ next_execution_date }}'),
              mcd.slot_ends_at
        ),
        NULL
      ) AS offline_ends_at_adjusted
  FROM rps_monitor_connectivity_data_deduplicated mcd
  LEFT JOIN `{{ params.project_id }}.cl._rps_vendor_special_days` sd ON mcd.created_date = sd.created_date
    AND mcd.entity_id = sd.entity_id
    AND mcd.vendor_code = sd.vendor_code
), rps_monitor_connectivity_adjusted_to_special_days_duration AS (
  SELECT created_date
    , region
    , country_code
    , entity_id
    , vendor_code
    , timezone
    , _operator_code
    , vendor_id
    , is_monitor_enabled
    , slot_starts_at
    , slot_ends_at
    , slot_duration
    , daily_schedule_duration
    , daily_schedule_duration_local
    , slots
    , slots_local
    , special_day_starts_at
    , special_day_ends_at
    , special_day_duration
    , daily_special_day_duration
    , daily_special_day_duration_local
    , IF(unreachable_starts_at_adjusted IS NOT NULL, is_unreachable, false) AS is_unreachable
    , unreachable_starts_at_adjusted AS unreachable_starts_at
    , unreachable_ends_at_adjusted AS unreachable_ends_at
    , TIMESTAMP_DIFF(unreachable_ends_at_adjusted, unreachable_starts_at_adjusted, SECOND) AS unreachable_duration
    , IF(offline_starts_at_adjusted IS NOT NULL, is_offline, false) AS is_offline
    , IF(offline_starts_at_adjusted IS NOT NULL, offline_reason, NULL) AS offline_reason
    , offline_starts_at_adjusted AS offline_starts_at
    , offline_ends_at_adjusted AS offline_ends_at
    , TIMESTAMP_DIFF(offline_ends_at_adjusted, offline_starts_at_adjusted, SECOND) AS offline_duration
  FROM rps_monitor_connectivity_adjusted_to_special_days
), rps_monitor_connectivity_data_struct AS (
  SELECT region
    , created_date
    , entity_id
    , country_code
    , vendor_code
    , timezone
    , _operator_code
    , vendor_id
    , is_monitor_enabled
    , slot_starts_at
    , slot_ends_at
    , slot_duration
    , daily_schedule_duration
    , daily_schedule_duration_local
    , special_day_starts_at
    , special_day_ends_at
    , daily_special_day_duration
    , daily_special_day_duration_local
    , slots
    , slots_local
    , STRUCT(
        is_unreachable
        , unreachable_starts_at
        , unreachable_ends_at
        , unreachable_duration
      ) AS connectivity_utc
    , STRUCT(
        is_offline
        , offline_reason
        , offline_starts_at
        , offline_ends_at
        , offline_duration
     ) AS availability_utc
    , STRUCT(
        is_unreachable
        , DATETIME(unreachable_starts_at, timezone) AS unreachable_starts_at
        , DATETIME(unreachable_ends_at, timezone) AS unreachable_ends_at
        , unreachable_duration
      ) AS connectivity_local
    , STRUCT(
        is_offline
        , offline_reason
        , DATETIME(offline_starts_at, timezone) AS offline_starts_at
        , DATETIME(offline_ends_at, timezone) AS offline_ends_at
        , offline_duration
     ) AS availability_local
  FROM rps_monitor_connectivity_adjusted_to_special_days_duration
)
SELECT region
    , entity_id
    , UPPER(country_code) AS country_iso
    , country_code
    , vendor_code
    , vendor_id
    , _operator_code
    , timezone
    , is_monitor_enabled
    , ARRAY_AGG(
        STRUCT(slot_starts_at AS starts_at
          , slot_ends_at AS ends_at
          , slot_duration AS duration
          , slots AS daily_quantity
          , daily_schedule_duration
          , daily_special_day_duration
          , connectivity_utc AS connectivity
          , availability_utc AS availability
        ) ORDER BY slot_starts_at, slot_ends_at
      ) AS slots_utc
    , ARRAY_AGG(
        STRUCT(DATETIME(slot_starts_at, timezone) AS starts_at
          , DATETIME(slot_ends_at, timezone) AS ends_at
          , slot_duration AS duration
          , slots_local AS daily_quantity
          , daily_schedule_duration_local AS daily_schedule_duration
          , daily_special_day_duration_local AS daily_special_day_duration
          , connectivity_local AS connectivity
          , availability_local AS availability
        ) ORDER BY DATETIME(slot_starts_at, timezone), DATETIME(slot_ends_at, timezone)
      ) AS slots_local
FROM rps_monitor_connectivity_data_struct
GROUP BY 1,2,3,4,5,6,7,8,9
