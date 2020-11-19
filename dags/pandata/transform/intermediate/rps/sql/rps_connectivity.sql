WITH distinct_slots AS (
  SELECT DISTINCT
    rps_connectivity.entity_id,
    rps_connectivity.vendor_code,
    slots.starts_at,
    slots.ends_at,
    rps_connectivity.timezone,
    IFNULL(rps_connectivity.is_monitor_enabled, FALSE) AS is_monitor_enabled,
    slots.daily_quantity AS daily_slot_quantity,
    slots.daily_schedule_duration AS daily_schedule_duration_in_seconds,
    slots.daily_special_day_duration AS daily_special_day_duration_in_seconds,
  FROM `fulfillment-dwh-production.curated_data_shared.rps_connectivity` AS rps_connectivity
  LEFT JOIN UNNEST (rps_connectivity.slots_utc) AS slots
),

slots_agg_events AS (
  SELECT
    rps_connectivity.entity_id,
    rps_connectivity.vendor_code,
    slots.starts_at,
    slots.ends_at,
    ARRAY_AGG(
      STRUCT(
        slots.connectivity.is_unreachable,
        slots.availability.is_offline,

        slots.duration AS slot_duration_in_seconds,
        slots.connectivity.unreachable_duration AS unreachable_duration_in_seconds,
        slots.availability.offline_duration AS offline_duration_in_seconds,

        TIMESTAMP(DATETIME(slots.connectivity.unreachable_starts_at, rps_connectivity.timezone)) AS unreachable_started_at_local,
        slots.connectivity.unreachable_starts_at AS unreachable_started_at_utc,
        TIMESTAMP(DATETIME(slots.connectivity.unreachable_ends_at, rps_connectivity.timezone)) AS unreachable_ended_at_local,
        slots.connectivity.unreachable_ends_at AS unreachable_ended_at_utc,
        TIMESTAMP(DATETIME(slots.availability.offline_starts_at, rps_connectivity.timezone)) AS offline_started_at_local,
        slots.availability.offline_starts_at AS offline_started_at_utc,
        TIMESTAMP(DATETIME(slots.availability.offline_ends_at, rps_connectivity.timezone)) AS offline_ended_at_local,
        slots.availability.offline_ends_at AS offline_ended_at_utc
      )
    ) AS events,
  FROM `fulfillment-dwh-production.curated_data_shared.rps_connectivity` AS rps_connectivity
  LEFT JOIN UNNEST (rps_connectivity.slots_utc) AS slots
  GROUP BY
    rps_connectivity.entity_id,
    rps_connectivity.vendor_code,
    slots.starts_at,
    slots.ends_at
)

SELECT
  (
    distinct_slots.vendor_code || '_'
    || distinct_slots.entity_id || '_'
    || distinct_slots.starts_at || '_'
  ) AS uuid,
  distinct_slots.entity_id,
  distinct_slots.vendor_code,
  distinct_slots.is_monitor_enabled,
  distinct_slots.daily_slot_quantity,
  distinct_slots.daily_schedule_duration_in_seconds,
  distinct_slots.daily_special_day_duration_in_seconds,
  TIMESTAMP(DATETIME(distinct_slots.starts_at, distinct_slots.timezone)) AS started_at_local,
  distinct_slots.starts_at AS started_at_utc,
  TIMESTAMP(DATETIME(distinct_slots.ends_at, distinct_slots.timezone)) AS ended_at_local,
  distinct_slots.ends_at AS ended_at_utc,
  DATE(distinct_slots.starts_at) AS created_date_utc,
  slots_agg_events.events,
FROM distinct_slots
LEFT JOIN slots_agg_events
       ON distinct_slots.entity_id = slots_agg_events.entity_id
      AND distinct_slots.vendor_code = slots_agg_events.vendor_code
      AND distinct_slots.starts_at = slots_agg_events.starts_at
