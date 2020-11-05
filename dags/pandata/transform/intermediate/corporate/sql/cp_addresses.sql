SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(user_id, rdbms_id) AS user_uuid,
  user_id,
  api_city_id,
  api_area_id,
  NULLIF(
    (
      IFNULL(address_line1, "") || IF(address_line1 IS NULL, "", "\n") ||
      IFNULL(address_line2, "") || IF(address_line2 IS NULL, "", "\n") ||
      IFNULL(address_line3, "") || IF(address_line3 IS NULL, "", "\n") ||
      IFNULL(address_line4, "") || IF(address_line4 IS NULL, "", "\n") ||
      IFNULL(address_line5, "")
    ),
  ""
  ) AS address,
  address_other,
  api_area_name,
  building,
  campus,
  city,
  country,
  delivery_instructions,
  district,
  entrance,
  floor,
  is_flexible_address,
  latitude,
  longitude,
  post_code,
  room,
  structure,
  type,
  timezone,
  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
  dwh_last_modified AS dwh_last_modified_utc,
FROM `{project_id}.pandata_raw_ml_corporate_latest.address`
