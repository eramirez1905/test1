WITH users_agg_roles AS (
  SELECT
    users_roles.user_uuid,
    ARRAY_AGG(
      STRUCT(
        roles.title,
        roles.landing_page,
        roles.priority,
        roles.order_list_headers,
        roles.has_audio_alert,
        roles.has_modal_alert,
        roles.has_vendor_role,
        roles.is_direct_order_assignment_allowed,
        roles.is_visible,
        roles.code_type,
        users_roles.created_at_local,
        users_roles.updated_at_local,
        users_roles.dwh_last_modified_at_utc
      )
    ) AS roles
  FROM `{project_id}.pandata_intermediate.pd_users_roles` AS users_roles
  LEFT JOIN `{project_id}.pandata_intermediate.pd_roles` AS roles
         ON users_roles.role_uuid = roles.uuid
  GROUP BY users_roles.user_uuid
)

SELECT
  users.uuid,
  users.id,
  users.rdbms_id,
  users.created_by_user_id,
  users.updated_by_user_id,
  users.email,
  users.first_name,
  users.last_name,
  users.phone,
  users.username,
  users.is_active,
  users.is_deleted,
  users.is_global,
  users.is_visible,
  users.is_automated,
  users.last_login_at_local,
  users.created_at_local,
  users.updated_at_local,
  users.dwh_last_modified_at_utc,
  users_agg_roles.roles,
FROM `{project_id}.pandata_intermediate.pd_users` AS users
LEFT JOIN users_agg_roles
       ON users.uuid = users_agg_roles.user_uuid
