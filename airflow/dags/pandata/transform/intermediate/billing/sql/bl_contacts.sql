SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(client_id, rdbms_id) AS client_uuid,
  client_id,

  CAST(email_notifiable AS BOOLEAN) AS is_email_notifiable,
  CAST(sms_notifiable AS BOOLEAN) AS is_sms_notifiable,

  name,
  email,
  phone_number,
  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
  dwh_last_modified AS dwh_last_modified_utc,
FROM `{project_id}.pandata_raw_ml_billing_latest.contact`
