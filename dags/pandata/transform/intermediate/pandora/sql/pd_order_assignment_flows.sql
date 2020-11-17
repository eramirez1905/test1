SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(order_id, rdbms_id) AS order_uuid,
  order_id,
  `{project_id}`.pandata_intermediate.PD_UUID(user_id, rdbms_id) AS user_uuid,
  user_id,
  `{project_id}`.pandata_intermediate.PD_UUID(assignee_id, rdbms_id) AS assignee_uuid,
  assignee_id,

  RANK() OVER (
    PARTITION BY
      rdbms_id,
      order_id
    ORDER BY date
  ) = 1 AS is_earliest_by_order,

  date AS created_at_local,
  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
  dwh_last_modified AS dwh_last_modified_at_utc
FROM `{project_id}.pandata_raw_ml_backend_latest.orderassignmentflows`
