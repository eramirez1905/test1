SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(role_id, rdbms_id) AS role_uuid,
  role_id,
  `{project_id}`.pandata_intermediate.PD_UUID(user_id, rdbms_id) AS user_uuid,
  user_id,

  created_at AS created_at_local,
  updated_at AS updated_at_local,
  dwh_last_modified AS dwh_last_modified_at_utc,
FROM `{project_id}.pandata_raw_ml_backend_latest.usersroles`
