WITH utr_target_periods_agg_history AS (
  SELECT
    utr_target_periods.country_code,
    utr_target_periods.zone_id,
    utr_target_periods.period_name,
    utr_target_periods.weekday,
    utr_target_periods.start_time,
    utr_target_periods.end_time,
    utr_target_periods.created_at,
    ARRAY_AGG(
      STRUCT(
        utr.updated_at AS updated_at_utc,
        utr.suggested AS suggested_utr,
        utr.utr,
        utr.is_latest
      )
    ) AS history
  FROM `fulfillment-dwh-production.curated_data_shared.utr_target_periods` AS utr_target_periods
  CROSS JOIN UNNEST (utr_target_periods.utr) AS utr
  GROUP BY
    utr_target_periods.country_code,
    utr_target_periods.zone_id,
    utr_target_periods.period_name,
    utr_target_periods.weekday,
    utr_target_periods.start_time,
    utr_target_periods.end_time,
    utr_target_periods.created_at
)

SELECT
  TO_HEX(
    SHA256(
      utr_target_periods.country_code || '_'
      || IFNULL(CAST(utr_target_periods.zone_id AS STRING), '_') || '_'
      || utr_target_periods.period_name || '_'
      || utr_target_periods.weekday || '_'
      || utr_target_periods.start_time || '_'
      || utr_target_periods.end_time || '_'
      || utr_target_periods.created_at
    )
  ) AS uuid,
  `{project_id}`.pandata_intermediate.LG_UUID(
    `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(utr_target_periods.country_code, utr_target_periods.city_id),
      utr_target_periods.country_code
  ) AS lg_city_uuid,
  utr_target_periods.city_id AS lg_city_id,
  `{project_id}`.pandata_intermediate.LG_UUID(utr_target_periods.zone_id, utr_target_periods.country_code) AS lg_zone_uuid,
  utr_target_periods.zone_id AS lg_zone_id,
  `{project_id}`.pandata_intermediate.EXCLUDE_T2(utr_target_periods.country_code) AS country_code,
  REGEXP_REPLACE(TRIM(LOWER(utr_target_periods.period_name)), '\\s', '_') AS period_name,
  utr_target_periods.weekday,
  utr_target_periods.start_time AS start_time_local,
  utr_target_periods.end_time AS end_time_local,
  utr_target_periods.timezone,
  utr_target_periods.created_at AS created_at_utc,
  utr_target_periods_agg_history.history,
FROM `fulfillment-dwh-production.curated_data_shared.utr_target_periods` AS utr_target_periods
LEFT JOIN utr_target_periods_agg_history
       ON utr_target_periods.country_code = utr_target_periods_agg_history.country_code
      AND utr_target_periods.zone_id = utr_target_periods_agg_history.zone_id
      AND utr_target_periods.period_name = utr_target_periods_agg_history.period_name
      AND utr_target_periods.weekday = utr_target_periods_agg_history.weekday
      AND utr_target_periods.start_time = utr_target_periods_agg_history.start_time
      AND utr_target_periods.end_time = utr_target_periods_agg_history.end_time
      AND utr_target_periods.created_at = utr_target_periods_agg_history.created_at
