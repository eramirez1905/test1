SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(customer_id, rdbms_id) AS customer_uuid,
  customer_id,

  CAST(is_deleted AS BOOLEAN) AS is_deleted,
  platform,
  dwh_last_modified AS dwh_last_modified_at_utc,
FROM `{project_id}.pandata_raw_ml_backend_latest.social_login`
