WITH blacklisted_phone_numbers AS (
  SELECT
    `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
    id,
    rdbms_id,
    SPLIT(phone_numbers, "\\n") AS phone_numbers,
    dwh_last_modified AS dwh_last_modified_at_utc,
  FROM `{project_id}.pandata_raw_ml_backend_latest.blacklisted_phonenumbers`
)

SELECT DISTINCT
  uuid || '_' || phone_number AS uuid,
  id,
  rdbms_id,
  phone_number,
  dwh_last_modified_at_utc,
FROM blacklisted_phone_numbers
CROSS JOIN UNNEST (phone_numbers) AS phone_number
