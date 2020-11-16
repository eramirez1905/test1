SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(created_by, rdbms_id) AS created_by_user_uuid,
  created_by AS created_by_user_id,
  `{project_id}`.pandata_intermediate.PD_UUID(updated_by, rdbms_id) AS updated_by_user_uuid,
  updated_by AS updated_by_user_id,
  `{project_id}`.pandata_intermediate.PD_UUID(language_id, rdbms_id) AS language_uuid,
  language_id,
  `{project_id}`.pandata_intermediate.PD_UUID(loyaltyprogramname_id, rdbms_id) AS loyalty_program_name_uuid,
  loyaltyprogramname_id AS loyalty_program_name_id,

  CAST(deleted AS BOOLEAN) AS is_deleted,
  CAST(guest AS BOOLEAN) AS is_guest,
  IFNULL(mobile_verified = 1, FALSE) AS is_mobile_verified,
  code,

  email,
  firstname AS first_name,
  lastname AS last_name,
  internal_comment,
  landline,
  mobile,
  mobile_confirmation_code,
  mobile_country_code,
  mobile_verified_attempts,
  source,
  user_agent AS last_user_agent,

  lastlogin AS last_login_at_local,
  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
  dwh_last_modified AS dwh_last_modified_at_utc,
FROM `{project_id}.pandata_raw_ml_backend_latest.customers`