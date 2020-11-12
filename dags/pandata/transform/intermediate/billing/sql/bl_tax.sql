SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,

  percentage,
  title,

  CAST(is_client_level AS BOOLEAN) AS is_client_level,
  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
  dwh_last_modified AS dwh_last_modified_utc,
FROM `{project_id}.pandata_raw_ml_billing_latest.tax`
