SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  option_id,
  value AS vat_rate,
  name
FROM `{project_id}.pandata_raw_ml_backend_latest.option_value`
