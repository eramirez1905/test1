SELECT
  `{project_id}`.pandata_intermediate.LG_UUID(unassigned_shift_id, country_code) AS uuid,
  unassigned_shift_id AS id,
  `{project_id}`.pandata_intermediate.LG_UUID(
    `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(country_code, city_id),
    country_code
  ) AS lg_city_uuid,
  `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(country_code, city_id) AS lg_city_id,
  `{project_id}`.pandata_intermediate.LG_UUID(zone_id, country_code) AS lg_zone_uuid,
  zone_id AS lg_zone_id,
  `{project_id}`.pandata_intermediate.LG_UUID(starting_point_id, country_code) AS lg_starting_point_uuid,
  starting_point_id AS lg_starting_point_id,
  `{project_id}`.pandata_intermediate.EXCLUDE_T2(country_code) AS country_code,

  NULLIF(created_by, 0) AS created_by_lg_user_id,
  NULLIF(updated_by, 0) AS updated_by_lg_user_id,

  created_by = 0 AS is_created_automatically,
  updated_by = 0 AS is_updated_automatically,

  state = 'PUBLISHED' AS is_state_published,
  state = 'PENDING' AS is_state_pending,
  state = 'CANCELLED' AS is_state_cancelled,

  tag = 'AUTOMATIC' AS is_tag_automatic,
  tag = 'COPIED' AS is_tag_copied,
  tag = 'TERMINATION' AS is_tag_termination,
  tag = 'STARTING_POINT_UNASSIGNED' AS is_tag_starting_point_unassigned,
  tag = 'MANUAL' AS is_tag_manual,
  tag = 'SWAP' AS is_tag_swap,

  slots,
  tag,
  state,

  timezone,
  TIMESTAMP(DATETIME(start_at, timezone)) AS start_at_local,
  start_at AS start_at_utc,
  TIMESTAMP(DATETIME(end_at, timezone)) AS end_at_local,
  end_at AS end_at_utc,
  TIMESTAMP(DATETIME(created_at, timezone)) AS created_at_local,
  created_at AS created_at_utc,
  TIMESTAMP(DATETIME(updated_at, timezone)) AS updated_at_local,
  updated_at AS updated_at_utc,
  created_date AS created_date_utc,
FROM `fulfillment-dwh-production.curated_data_shared.unassigned_shifts`
