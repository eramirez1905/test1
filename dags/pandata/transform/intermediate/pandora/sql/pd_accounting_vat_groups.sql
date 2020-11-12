SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(accounting_id, rdbms_id) AS accounting_uuid,
  accounting_id,

  amount AS amount_local,
  base_amount AS base_amount_local,
  percentage,

  dwh_last_modified AS dwh_last_modified_at_utc
FROM `{project_id}.pandata_raw_ml_backend_latest.accounting_vat_groups`
