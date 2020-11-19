SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(company_id, rdbms_id) AS company_uuid,
  company_id,

  name,
  state,
  state = 'active' AS is_state_active,
  state = 'deleted' AS is_state_deleted,

  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
FROM `{project_id}.pandata_raw_ml_corporate_latest.company_department`
