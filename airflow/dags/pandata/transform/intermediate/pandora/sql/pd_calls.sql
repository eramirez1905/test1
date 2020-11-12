SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(created_by, rdbms_id) AS created_by_user_uuid,
  created_by AS created_by_user_id,
  `{project_id}`.pandata_intermediate.PD_UUID(updated_by, rdbms_id) AS updated_by_user_uuid,
  updated_by AS updated_by_user_id,
  -- object_id is a polymorphic field
  IF(type = 'order_notification',
    `{project_id}`.pandata_intermediate.PD_UUID(object_id, rdbms_id), NULL
  ) AS order_uuid,
  IF(type = 'order_notification', object_id, NULL) AS order_id,

  active = 1 AS is_active,
  attempt_number,
  api_used,
  fail_reason,
  phone_number,
  executed_at AS executed_at_local,
  scheduled_for AS scheduled_for_at_local,
  created_at AS created_at_local,
  updated_at AS updated_at_local,
  dwh_last_modified AS dwh_last_modified_at_utc,
FROM `{project_id}.pandata_raw_ml_backend_latest.calls`
