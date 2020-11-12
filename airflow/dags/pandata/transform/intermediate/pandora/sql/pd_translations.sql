SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(language_id, rdbms_id) AS language_uuid,
  language_id,
  `{project_id}`.pandata_intermediate.PD_UUID(object_id, rdbms_id) AS object_uuid,
  object_id,

  object AS object_type,
  objectattribute AS object_attribute,
  text AS object_text,

  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
  dwh_last_modified AS dwh_last_modified_at_utc,
FROM `{project_id}.pandata_raw_ml_backend_latest.translations`
