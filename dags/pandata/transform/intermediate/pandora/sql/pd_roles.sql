SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,

  title,
  landing_page,
  priority,
  order_list_headers,

  CAST(audio_alert AS BOOLEAN) AS has_audio_alert,
  CAST(modal_alert AS BOOLEAN) AS has_modal_alert,
  CAST(order_tracking_full_view AS BOOLEAN) AS has_order_tracking_full_view,
  IFNULL(code = 'vendor' OR code = 'vendor-backend', FALSE) AS has_vendor_role,
  CAST(is_direct_order_assignment_allowed AS BOOLEAN) AS is_direct_order_assignment_allowed,
  IFNULL(visible = 1, FALSE) AS is_visible,

  code AS code_type,

  created_at AS created_at_local,
  updated_at AS updated_at_local,
  dwh_last_modified AS dwh_last_modified_at_utc,
FROM `{project_id}.pandata_raw_ml_backend_latest.roles`
