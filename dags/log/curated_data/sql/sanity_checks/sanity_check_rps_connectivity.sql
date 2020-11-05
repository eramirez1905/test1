WITH entities AS (
  SELECT DISTINCT p.entity_id
  FROM `{{ params.project_id }}.cl.entities` en
  CROSS JOIN UNNEST(platforms) p
), rps_connectivity AS (
  SELECT entity_id
    , vendor_code
    , (SELECT MAX(starts_at) FROM UNNEST(slots_utc)) AS max_starts_at
    , (SELECT MAX(ends_at) FROM UNNEST(slots_utc)) AS max_ends_at
    , (SELECT COUNTIF(duration < 0 ) FROM UNNEST(slots_utc)) AS negative_slot_duration_ct
    , (SELECT COUNTIF(daily_schedule_duration < 0 ) FROM UNNEST(slots_utc)) AS negative_daily_schedule_duration_ct
    , (SELECT COUNTIF(daily_special_day_duration  < 0 ) FROM UNNEST(slots_utc)) AS negative_daily_special_day_duration_ct
    , (SELECT COUNTIF(connectivity.unreachable_duration < 0 ) FROM UNNEST(slots_utc)) AS negative_connectivity_unreachable_duration_ct
    , (SELECT COUNTIF(availability.offline_duration < 0 ) FROM UNNEST(slots_utc)) AS negative_availability_offline_duration_ct
  FROM `{{ params.project_id }}.cl.rps_connectivity`
), rps_connectivity_with_entity_id AS (
  SELECT COUNT(*) AS count
    , COUNT(en.entity_id) AS entity_id_count
    , COUNTIF(en.entity_id IS NOT NULL) AS valid_entity_id_count
    , TIMESTAMP_DIFF('{{ next_execution_date }}', MAX(con.max_ends_at), HOUR) AS latest_ends_at_diff
    , COUNTIF(con.max_starts_at > '{{ next_execution_date }}') AS future_slot_ct
    , SUM(negative_slot_duration_ct + negative_daily_schedule_duration_ct + negative_daily_special_day_duration_ct + negative_connectivity_unreachable_duration_ct + negative_availability_offline_duration_ct) AS negative_duration_cts
  FROM rps_connectivity con
  LEFT JOIN entities en ON con.entity_id = en.entity_id
)
SELECT (count = entity_id_count) AS null_entity_id_check
  , (count = valid_entity_id_count) AS valid_entity_id_check
  , (latest_ends_at_diff <= 24) AS timeliness_check
  , (future_slot_ct = 0) AS future_check
  , (negative_duration_cts = 0) AS negative_duration_check
FROM rps_connectivity_with_entity_id
