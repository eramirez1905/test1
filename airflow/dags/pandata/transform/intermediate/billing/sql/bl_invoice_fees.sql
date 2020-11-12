SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(fee_id, rdbms_id) AS fee_uuid,
  fee_id,
  `{project_id}`.pandata_intermediate.PD_UUID(invoice_id, rdbms_id) AS invoice_uuid,
  invoice_id,

  CAST(is_tax_applicable AS BOOLEAN) AS is_tax_applicable,
  quantity,
  single_amount AS single_amount_local,

  category_name AS category_type,
  description,

  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
  dwh_last_modified AS dwh_last_modified_utc
FROM `{project_id}.pandata_raw_ml_billing_latest.invoice_fee`
