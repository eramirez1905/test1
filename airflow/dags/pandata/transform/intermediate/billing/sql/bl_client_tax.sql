SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(tax_id, rdbms_id) AS tax_uuid,
  tax_id,
  `{project_id}`.pandata_intermediate.PD_UUID(client_id, rdbms_id) AS client_uuid,
  client_id,
  rdbms_id,
  dwh_last_modified AS dwh_last_modified_utc,
FROM `{project_id}.pandata_raw_ml_billing_latest.client_tax`
