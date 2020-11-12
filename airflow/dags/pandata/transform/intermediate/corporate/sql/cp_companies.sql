SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  city,
  name,
  industry,
  postal_code,
  purpose,
  setting,
  state,
  street,

  state = 'active' AS is_state_active,
  state = 'deleted' AS is_state_deleted,
  state = 'new' AS is_state_new,
  state = 'suspended' AS is_state_suspended,

  CAST(is_allowance_enabled AS BOOLEAN) AS is_allowance_enabled,
  IFNULL(is_requested_demo = 1, FALSE) AS is_requested_demo,
  IFNULL(is_self_signup = 1, FALSE) AS is_self_signup,
  CAST(require_account_linking AS BOOLEAN) AS is_account_linking_required,

  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
  dwh_last_modified AS dwh_last_modified_utc,
FROM `{project_id}.pandata_raw_ml_corporate_latest.company`
