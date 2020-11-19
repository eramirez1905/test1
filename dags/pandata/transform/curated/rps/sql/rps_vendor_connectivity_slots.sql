SELECT
  vendor_connectivity_slots.uuid,
  countries.rdbms_id,
  vendor_connectivity_slots.entity_id,
  vendor_connectivity_slots.vendor_code,
  vendor_connectivity_slots.is_monitor_enabled,
  vendor_connectivity_slots.daily_slot_quantity,
  vendor_connectivity_slots.daily_schedule_duration_in_seconds,
  vendor_connectivity_slots.daily_special_day_duration_in_seconds,
  vendor_connectivity_slots.started_at_local,
  vendor_connectivity_slots.started_at_utc,
  vendor_connectivity_slots.ended_at_local,
  vendor_connectivity_slots.ended_at_utc,
  vendor_connectivity_slots.created_date_utc,
  vendor_connectivity_slots.events,
FROM `{project_id}.pandata_intermediate.rps_connectivity` AS vendor_connectivity_slots
LEFT JOIN `{project_id}.pandata_intermediate.pd_countries` AS countries
       ON vendor_connectivity_slots.entity_id = countries.entity_id
