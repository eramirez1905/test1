SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(discount_id, rdbms_id) AS discount_uuid,
  discount_id,
  PARSE_TIME("%T", start_time) AS start_time,
  PARSE_TIME("%T", end_time) AS end_time,
  weekday,
FROM `{project_id}.pandata_raw_ml_backend_latest.discount_schedule`
