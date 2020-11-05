SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(parent_id, rdbms_id) AS parent_uuid,
  parent_id,

  title,
  code,
  google_map_lang AS google_map_language,
  locale,

  CAST(active AS BOOLEAN) AS is_active,
  CAST(deleted AS BOOLEAN) AS is_deleted,
  CAST(text_direction_right_to_left AS BOOLEAN) AS is_text_direction_right_to_left,

  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
  dwh_last_modified AS dwh_last_modified_at_utc,
FROM `{project_id}.pandata_raw_ml_backend_latest.languages`
