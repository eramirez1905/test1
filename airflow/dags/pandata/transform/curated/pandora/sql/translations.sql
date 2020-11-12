SELECT
  uuid,
  id,
  rdbms_id,
  language_id,
  object_id,
  object_type,
  object_attribute,
  object_text,
  created_at_utc,
  updated_at_utc,
  dwh_last_modified_at_utc,
FROM `{project_id}.pandata_intermediate.pd_translations`
