-- all the original date fields have some data error
-- since most of them are 1970-01-01 even though the accompanying
-- utc timestamp is not 1970-01-01
SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(client_id, rdbms_id) AS client_uuid,
  client_id,

  CAST(is_on_separate_invoice AS BOOLEAN) AS is_on_separate_invoice,
  CAST(is_tax_applicable AS BOOLEAN) AS is_tax_applicable,

  description,
  quantity,
  category AS category_type,
  billing_frequency,
  single_amount AS single_amount_local,

  billing_end_date_utc AS billing_end_at_utc,
  billing_start_date_utc AS billing_start_at_utc,
  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
  dwh_last_modified AS dwh_last_modified_utc,
FROM `{project_id}.pandata_raw_ml_billing_latest.fee`
