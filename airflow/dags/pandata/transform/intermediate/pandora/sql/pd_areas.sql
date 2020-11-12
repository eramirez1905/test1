SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(area_id, rdbms_id) AS area_uuid,
  area_id,
  `{project_id}`.pandata_intermediate.PD_UUID(city_id, rdbms_id) AS city_uuid,
  city_id,
  title AS name,
  active = 1 AS is_active,
  lat AS latitude,
  lon AS longitude,
  postcode,
  zoom,
  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
  dwh_last_modified AS dwh_last_modified_at_utc
FROM `{project_id}.pandata_raw_ml_backend_latest.areas`
