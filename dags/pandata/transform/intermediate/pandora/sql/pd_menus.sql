SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(vendor_id, rdbms_id) AS vendor_uuid,
  vendor_id,

  available_weekdays,
  code,
  description,
  position,
  title,
  menu_type,

  IFNULL(active = 1, FALSE) AS is_active,
  CAST(deleted AS BOOLEAN) AS is_deleted,

  PARSE_TIME("%T", IF(start_hour = '24:00:00', '00:00:00', start_hour)) AS start_time_local,
  PARSE_TIME("%T", IF(stop_hour = '24:00:00', '23:59:00', stop_hour)) AS stop_time_local,

  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
  dwh_last_modified AS dwh_last_modified_at_utc,
FROM `{project_id}.pandata_raw_ml_backend_latest.menus`
