WITH users_agg_roles AS (
  SELECT
    users.id AS user_id,
    users.rdbms_id,
    IFNULL(LOGICAL_OR(roles.code LIKE '%vendor%'), FALSE) AS has_vendor_role,
  FROM `{project_id}.pandata_raw_ml_backend_latest.users` AS users
  LEFT JOIN `{project_id}.pandata_raw_ml_backend_latest.usersroles` AS user_roles
         ON user_roles.rdbms_id = users.rdbms_id
        AND user_roles.user_id = users.id
  LEFT JOIN `{project_id}.pandata_raw_ml_backend_latest.roles` AS roles
         ON roles.rdbms_id = user_roles.rdbms_id
        AND roles.id = user_roles.role_id
  GROUP BY
    users.id,
    users.rdbms_id
)

SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(users.id, users.rdbms_id) AS uuid,
  users.id,
  users.rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(users.created_by, users.rdbms_id) AS created_by_user_uuid,
  users.created_by AS created_by_user_id,
  `{project_id}`.pandata_intermediate.PD_UUID(users.updated_by, users.rdbms_id) AS updated_by_user_uuid,
  users.updated_by AS updated_by_user_id,

  users.email,
  users.firstname AS first_name,
  users.lastname AS last_name,
  users.phone,
  users.username,

  CAST(users.active AS BOOLEAN) AS is_active,
  IFNULL(users.deleted = 1, FALSE) AS is_deleted,
  CAST(users.global AS BOOLEAN) AS is_global,
  CAST(users.visible AS BOOLEAN) AS is_visible,
  users.username IN (
    'core-service',
    'systemcron',
    'urbanninja',
    'vendor-backend',
    'wswebpanda',
    'logistics'
  ) AS is_automated,
  users_agg_roles.has_vendor_role,

  users.last_login_at AS last_login_at_local,
  users.created_at AS created_at_local,
  users.updated_at AS updated_at_local,
  users.dwh_last_modified AS dwh_last_modified_at_utc,
FROM `{project_id}.pandata_raw_ml_backend_latest.users` AS users
LEFT JOIN users_agg_roles
       ON users.rdbms_id = users_agg_roles.rdbms_id
      AND users.id = users_agg_roles.user_id
