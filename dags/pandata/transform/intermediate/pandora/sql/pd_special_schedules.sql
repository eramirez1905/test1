SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(vendor_id, rdbms_id) AS vendor_uuid,
  vendor_id,

  -- "24:00:00" will throw an error if cast to TIME
  PARSE_TIME('%T', IF(start_hour = "24:00:00", "00:00:00", start_hour)) AS start_time_local,
  PARSE_TIME('%T', IF(stop_hour = "24:00:00", "23:59:59", stop_hour)) AS stop_time_local,

  type,
  type = 'delivering' AS is_type_delivering,
  type = 'opened' AS is_type_opened,
  type = 'closed' AS is_type_closed,
  type = 'busy' AS is_type_busy,
  type = 'unavailable' AS is_type_unavailable,
  SAFE_CAST(all_day AS BOOLEAN) AS is_all_day,
  date AS start_date_local,
  end_date AS end_date_local,
  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
  dwh_last_modified AS dwh_last_modified_at_utc,
FROM `{project_id}.pandata_raw_ml_backend_latest.specialdays`
