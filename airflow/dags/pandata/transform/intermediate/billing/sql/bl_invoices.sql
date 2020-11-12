-- all the original date fields have some data error
-- since most of them are 1970-01-01 even though the accompanying
-- utc timestamp is not 1970-01-01
SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(bill_run_id, rdbms_id) AS bill_run_uuid,
  bill_run_id,
  `{project_id}`.pandata_intermediate.PD_UUID(client_id, rdbms_id) AS client_uuid,
  client_id,
  `{project_id}`.pandata_intermediate.PD_UUID(commission_id, rdbms_id) AS commission_uuid,
  commission_id,
  email_message_id,

  amount AS amount_local,
  invoice_number,
  calculated_values,
  current_status_name AS status,
  invoice_details_file_path,
  invoice_generation_mode AS invoice_generation_type,
  invoice_generation_mode = 'AUTOMATIC' AS is_invoice_generation_type_automatic,
  invoice_generation_mode = 'MANUAL' AS is_invoice_generation_type_manual,

  invoicing_date_utc AS invoicing_at_utc,
  from_date_utc AS from_utc,
  until_date_utc AS until_utc,
  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
  dwh_last_modified AS dwh_last_modified_utc
FROM `{project_id}.pandata_raw_ml_billing_latest.invoice`
