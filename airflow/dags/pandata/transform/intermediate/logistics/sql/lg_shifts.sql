WITH shifts_agg_break_times AS (
  SELECT
    shifts.country_code,
    shifts.shift_id,
    ARRAY_AGG(
      STRUCT(
        break_times.details.performed_by AS performed_by_type,
        break_times.details.type,
        break_times.details.reason,
        break_times.details.comment,
        break_times.duration AS duration_in_seconds,
        break_times.start_at_local,
        break_times.end_at_local,
        break_times.start_at AS start_at_utc,
        break_times.end_at AS end_at_utc
      )
    ) AS break_times
  FROM `fulfillment-dwh-production.curated_data_shared.shifts` AS shifts
  CROSS JOIN UNNEST (shifts.break_time) AS break_times
  GROUP BY
    shifts.country_code,
    shifts.shift_id
),

shifts_agg_evaluations AS (
  SELECT
    shifts.country_code,
    shifts.shift_id,
    ARRAY_AGG(
      STRUCT(
        `{project_id}`.pandata_intermediate.LG_UUID(evaluations.id, shifts.country_code) AS uuid,
        evaluations.id,
        evaluations.status,
        evaluations.vehicle.name AS vehicle_name,
        evaluations.duration AS duration_in_seconds,
        evaluations.start_at_local,
        evaluations.end_at_local,
        evaluations.start_at AS start_at_utc,
        evaluations.end_at AS end_at_utc
      )
    ) AS evaluations
  FROM `fulfillment-dwh-production.curated_data_shared.shifts` AS shifts
  CROSS JOIN UNNEST (shifts.evaluations) AS evaluations
  GROUP BY
    shifts.country_code,
    shifts.shift_id
),

shifts_agg_actual_break_time_by_date AS (
  SELECT
    shifts.country_code,
    shifts.shift_id,
    ARRAY_AGG(
      STRUCT(
        actual_break_time_by_date.duration AS duration_in_seconds,
        actual_break_time_by_date.day AS date_utc
      )
    ) AS actual_break_time_by_date
  FROM `fulfillment-dwh-production.curated_data_shared.shifts` AS shifts
  CROSS JOIN UNNEST (shifts.actual_break_time_by_date) AS actual_break_time_by_date
  GROUP BY
    shifts.country_code,
    shifts.shift_id
),

shifts_agg_actual_working_time_by_date AS (
  SELECT
    shifts.country_code,
    shifts.shift_id,
    ARRAY_AGG(
      STRUCT(
        vehicle_name,
        accepted_deliveries_count AS deliveries_accepted_count,
        notified_deliveries_count AS deliveries_notified_count,
        actual_working_time_by_date.status,
        actual_working_time_by_date.day AS date_utc
      )
    ) AS actual_working_time_by_date
  FROM `fulfillment-dwh-production.curated_data_shared.shifts` AS shifts
  CROSS JOIN UNNEST (shifts.actual_working_time_by_date) AS actual_working_time_by_date
  GROUP BY
    shifts.country_code,
    shifts.shift_id
),

shifts_agg_absences AS (
  SELECT
    shifts.country_code,
    shifts.shift_id,
    ARRAY_AGG(
      STRUCT(
        `{project_id}`.pandata_intermediate.LG_UUID(absences.id, shifts.country_code) AS uuid,
        absences.id,
        `{project_id}`.pandata_intermediate.LG_UUID(absences.violation_id, shifts.country_code) AS lg_violation_uuid,
        absences.violation_id AS lg_violation_id,
        absences.status,
        absences.reason,
        absences.comment,
        absences.is_paid,
        absences.start_at AS start_at_utc,
        absences.end_at AS end_at_utc
      )
    ) AS absences
  FROM `fulfillment-dwh-production.curated_data_shared.shifts` AS shifts
  CROSS JOIN UNNEST (shifts.absences) AS absences
  GROUP BY
    shifts.country_code,
    shifts.shift_id
)

SELECT
  `{project_id}`.pandata_intermediate.LG_UUID(shifts.shift_id, shifts.country_code) AS uuid,
  shifts.shift_id AS id,
  `{project_id}`.pandata_intermediate.LG_UUID(shifts.rider_id, shifts.country_code) AS lg_rider_uuid,
  shifts.rider_id AS lg_rider_id,
  `{project_id}`.pandata_intermediate.LG_UUID(
    `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(shifts.country_code, shifts.city_id),
      shifts.country_code
  ) AS lg_city_uuid,
  `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(shifts.country_code, shifts.city_id) AS lg_city_id,
  `{project_id}`.pandata_intermediate.LG_UUID(shifts.starting_point_id, shifts.country_code) AS lg_starting_point_uuid,
  shifts.starting_point_id AS lg_starting_point_id,
  `{project_id}`.pandata_intermediate.LG_UUID(shifts.zone_id, shifts.country_code) AS lg_zone_uuid,
  shifts.zone_id AS lg_zone_id,
  `{project_id}`.pandata_intermediate.LG_UUID(shifts.shift_created_by, shifts.country_code) AS created_by_lg_user_uuid,
  shifts.shift_created_by AS created_by_lg_user_id,
  `{project_id}`.pandata_intermediate.LG_UUID(shifts.shift_updated_by, shifts.country_code) AS updated_by_lg_user_uuid,
  shifts.shift_updated_by AS updated_by_lg_user_id,

  `{project_id}`.pandata_intermediate.EXCLUDE_T2(shifts.country_code) AS country_code,

  shifts.is_repeating,

  shifts.shift_state AS state,
  shifts.shift_tag AS tag,
  shifts.vehicle_bag,
  shifts.vehicle_profile,
  deliveries.accepted AS deliveries_accepted_count,
  deliveries.notified AS deliveries_notified_count,

  shifts.planned_shift_duration AS planned_shift_duration_in_seconds,
  shifts.login_difference AS login_difference_in_seconds,
  shifts.logout_difference AS logout_difference_in_seconds,
  shifts.actual_working_time AS actual_working_time_in_seconds,
  shifts.actual_break_time AS actual_break_time_in_seconds,

  shifts.timezone,
  shifts.shift_start_at AS start_at_utc,
  shifts.shift_end_at AS end_at_utc,
  shifts.actual_start_at AS actual_start_at_utc,
  shifts.actual_end_at AS actual_end_at_utc,
  shifts.created_date AS created_date_utc,
  shifts.created_at AS created_at_utc,
  shifts.updated_at AS updated_at_utc,
  shifts_agg_absences.absences,
  shifts_agg_actual_working_time_by_date.actual_working_time_by_date,
  shifts_agg_actual_break_time_by_date.actual_break_time_by_date,
  shifts_agg_evaluations.evaluations,
  shifts_agg_break_times.break_times,
FROM `fulfillment-dwh-production.curated_data_shared.shifts` AS shifts
LEFT JOIN shifts_agg_absences
       ON shifts.country_code = shifts_agg_absences.country_code
      AND shifts.shift_id = shifts_agg_absences.shift_id
LEFT JOIN shifts_agg_actual_working_time_by_date
       ON shifts.country_code = shifts_agg_actual_working_time_by_date.country_code
      AND shifts.shift_id = shifts_agg_actual_working_time_by_date.shift_id
LEFT JOIN shifts_agg_actual_break_time_by_date
       ON shifts.country_code = shifts_agg_actual_break_time_by_date.country_code
      AND shifts.shift_id = shifts_agg_actual_break_time_by_date.shift_id
LEFT JOIN shifts_agg_evaluations
       ON shifts.country_code = shifts_agg_evaluations.country_code
      AND shifts.shift_id = shifts_agg_evaluations.shift_id
LEFT JOIN shifts_agg_break_times
       ON shifts.country_code = shifts_agg_break_times.country_code
      AND shifts.shift_id = shifts_agg_break_times.shift_id
