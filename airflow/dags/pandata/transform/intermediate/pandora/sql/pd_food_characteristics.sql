SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,

  title,
  IFNULL(active = 1, FALSE) AS is_active,
  CAST(halal AS BOOLEAN) AS is_halal,
  CAST(vegetarian AS BOOLEAN) AS is_vegetarian,
  CAST(mobile_filter AS BOOLEAN) AS has_mobile_filter,

  dwh_last_modified AS dwh_last_modified_at_utc,
FROM `{project_id}.pandata_raw_ml_backend_latest.foodcaracteristics`
