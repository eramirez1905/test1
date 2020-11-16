SELECT
  delivery_areas_events.uuid,
  countries.rdbms_id,
  delivery_areas_events.lg_event_uuid,
  delivery_areas_events.lg_event_id,
  delivery_areas_events.lg_transaction_uuid,
  delivery_areas_events.lg_transaction_id,
  delivery_areas_events.lg_end_transaction_uuid,
  delivery_areas_events.lg_end_transaction_id,
  delivery_areas_events.lg_city_uuid,
  delivery_areas_events.lg_city_id,
  delivery_areas_events.lg_zone_uuid,
  delivery_areas_events.lg_zone_id,
  delivery_areas_events.country_code,
  delivery_areas_events.is_active,
  delivery_areas_events.is_shape_in_sync,
  delivery_areas_events.operation_type,
  delivery_areas_events.action_type,
  delivery_areas_events.city_name,
  delivery_areas_events.value,
  delivery_areas_events.activation_threshold,
  delivery_areas_events.title,
  delivery_areas_events.shape_geo,
  delivery_areas_events.duration_in_seconds,
  delivery_areas_events.timezone,
  delivery_areas_events.active_from_utc,
  delivery_areas_events.active_to_utc,
  delivery_areas_events.start_at_utc,
  delivery_areas_events.end_at_utc,
  delivery_areas_events.created_date_utc,
  delivery_areas_events.tags,
  delivery_areas_events.message,
FROM `{project_id}.pandata_intermediate.lg_delivery_areas_events` AS delivery_areas_events
LEFT JOIN `{project_id}.pandata_intermediate.pd_countries` AS countries
       ON delivery_areas_events.country_code = countries.lg_country_code