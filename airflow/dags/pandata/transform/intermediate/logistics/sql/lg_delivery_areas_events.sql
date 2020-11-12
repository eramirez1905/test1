SELECT
  (
    delivery_areas_events.transaction_id || '_' ||
    delivery_areas_events.event_id || '_' ||
    delivery_areas_events.country_code
  ) AS uuid,
  `{project_id}`.pandata_intermediate.LG_UUID(delivery_areas_events.event_id, delivery_areas_events.country_code) AS lg_event_uuid,
  delivery_areas_events.event_id AS lg_event_id,
  `{project_id}`.pandata_intermediate.LG_UUID(delivery_areas_events.transaction_id, delivery_areas_events.country_code) AS lg_transaction_uuid,
  delivery_areas_events.transaction_id AS lg_transaction_id,
  `{project_id}`.pandata_intermediate.LG_UUID(delivery_areas_events.end_transaction_id, delivery_areas_events.country_code) AS lg_end_transaction_uuid,
  delivery_areas_events.end_transaction_id AS lg_end_transaction_id,
  `{project_id}`.pandata_intermediate.LG_UUID(
    `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(delivery_areas_events.country_code, delivery_areas_events.city.city_id),
    delivery_areas_events.country_code
  ) AS lg_city_uuid,
  `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(delivery_areas_events.country_code, delivery_areas_events.city.city_id) AS lg_city_id,
  `{project_id}`.pandata_intermediate.LG_UUID(delivery_areas_events.zone_id, delivery_areas_events.country_code) AS lg_zone_uuid,
  delivery_areas_events.zone_id AS lg_zone_id,

  `{project_id}`.pandata_intermediate.EXCLUDE_T2(delivery_areas_events.country_code) AS country_code,

  delivery_areas_events.is_active,
  delivery_areas_events.is_shape_in_sync,

  delivery_areas_events.operation_type,
  delivery_areas_events.action AS action_type,

  delivery_areas_events.city.name AS city_name,
  delivery_areas_events.value,
  delivery_areas_events.activation_threshold,
  delivery_areas_events.title,
  delivery_areas_events.shape AS shape_geo,
  delivery_areas_events.duration AS duration_in_seconds,

  delivery_areas_events.timezone,
  delivery_areas_events.active_from AS active_from_utc,
  delivery_areas_events.active_to AS active_to_utc,
  delivery_areas_events.start_at AS start_at_utc,
  delivery_areas_events.end_at AS end_at_utc,
  delivery_areas_events.created_date AS created_date_utc,
  delivery_areas_events.tags,
  delivery_areas_events.message,

FROM `fulfillment-dwh-production.curated_data_shared.delivery_areas_events` AS delivery_areas_events
