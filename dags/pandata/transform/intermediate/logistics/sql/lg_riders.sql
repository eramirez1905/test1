WITH riders_agg_feedbacks AS (
  SELECT
    riders.country_code,
    riders.rider_id,
    ARRAY_AGG(
      STRUCT(
        feedbacks.user_id AS lg_user_id,
        `{project_id}`.pandata_intermediate.LG_UUID(feedbacks.user_id, riders.country_code) AS lg_user_uuid,
        feedbacks.order_id AS lg_order_id,
        `{project_id}`.pandata_intermediate.LG_UUID(feedbacks.order_id, riders.country_code) AS lg_order_uuid,
        feedbacks.reason,
        feedbacks.comment,
        feedbacks.created_at AS created_at_utc
      )
    ) AS feedbacks
  FROM `fulfillment-dwh-production.curated_data_shared.riders` AS riders
  CROSS JOIN UNNEST (riders.rider_feedbacks) AS feedbacks
  GROUP BY
    riders.country_code,
    riders.rider_id
),

riders_agg_starting_points AS (
  SELECT
    riders.country_code,
    riders.rider_id,
    ARRAY_AGG(
      STRUCT(
        starting_points.id,
        `{project_id}`.pandata_intermediate.LG_UUID(starting_points.id, riders.country_code) AS uuid
      )
    ) AS starting_points
  FROM `fulfillment-dwh-production.curated_data_shared.riders` AS riders
  CROSS JOIN UNNEST (riders.starting_points) AS starting_points
  GROUP BY
    riders.country_code,
    riders.rider_id
),

riders_agg_vehicles AS (
  SELECT
    riders.country_code,
    riders.rider_id,
    ARRAY_AGG(
      STRUCT(
        vehicles.vehicle_type_id AS lg_vehicle_type_id,
        vehicles.created_by AS created_by_lg_user_id,
        `{project_id}`.pandata_intermediate.LG_UUID(vehicles.created_by, riders.country_code) AS created_by_lg_user_uuid,
        vehicles.updated_by AS updated_by_lg_user_id,
        `{project_id}`.pandata_intermediate.LG_UUID(vehicles.updated_by, riders.country_code) AS updated_by_lg_user_uuid,
        vehicles.name,
        vehicles.profile,
        vehicles.created_at AS created_at_utc,
        vehicles.updated_at AS updated_at_utc
      )
    ) AS vehicles
  FROM `fulfillment-dwh-production.curated_data_shared.riders` AS riders
  CROSS JOIN UNNEST (riders.rider_vehicles) AS vehicles
  GROUP BY
    riders.country_code,
    riders.rider_id
),

riders_agg_absences_history AS (
  SELECT
    riders.country_code,
    riders.rider_id,
    ARRAY_AGG(
      STRUCT(
        absences_history.id,
        `{project_id}`.pandata_intermediate.LG_UUID(absences_history.id, riders.country_code) AS uuid,
        absences_history.user_id AS lg_user_id,
        `{project_id}`.pandata_intermediate.LG_UUID(absences_history.user_id, riders.country_code) AS lg_user_uuid,
        absences_history.violation_id AS lg_violation_id,
        `{project_id}`.pandata_intermediate.LG_UUID(absences_history.violation_id, riders.country_code) AS lg_violation_uuid,
        absences_history.reason,
        absences_history.comment,
        absences_history.is_paid,
        absences_history.status,
        absences_history.start_at AS start_at_utc,
        absences_history.end_at AS end_at_utc,
        absences_history.created_at AS created_at_utc,
        absences_history.updated_at AS updated_at_utc
      )
    ) AS absences_history
  FROM `fulfillment-dwh-production.curated_data_shared.riders` AS riders
  CROSS JOIN UNNEST (riders.absences_history) AS absences_history
  GROUP BY
    riders.country_code,
    riders.rider_id
),

riders_agg_batches AS (
  SELECT
    riders.country_code,
    riders.rider_id,
    ARRAY_AGG(
      STRUCT(
        batches.id,
        `{project_id}`.pandata_intermediate.LG_UUID(batches.id, riders.country_code) AS uuid,
        batches.number,
        batches.active_from AS active_from_utc,
        batches.active_until AS active_until_utc
      )
    ) AS batches
  FROM `fulfillment-dwh-production.curated_data_shared.riders` AS riders
  CROSS JOIN UNNEST (riders.batches) AS batches
  GROUP BY
    riders.country_code,
    riders.rider_id
),

contracts_agg_interval_rules AS (
  SELECT
    riders.country_code,
    riders.rider_id,
    contracts.id AS contract_id,
    ARRAY_AGG(
      STRUCT(
        interval_rules.id,
        interval_rules.interval_rule_type AS type,
        interval_rules.interval_type,
        interval_rules.interval_period,
        interval_rules.amount,
        interval_rules.created_at AS created_at_utc,
        interval_rules.updated_at AS updated_at_utc
      )
    ) AS interval_rules
  FROM `fulfillment-dwh-production.curated_data_shared.riders` AS riders
  CROSS JOIN UNNEST (riders.contracts) AS contracts
  CROSS JOIN UNNEST (contracts.interval_rules) AS interval_rules
  GROUP BY
    riders.country_code,
    riders.rider_id,
    contracts.id
),

riders_agg_contracts AS (
  SELECT
    riders.country_code,
    riders.rider_id,
    ARRAY_AGG(
      STRUCT(
        contracts.id,
        `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(riders.country_code, contracts.city_id) AS lg_city_id,
        `{project_id}`.pandata_intermediate.LG_UUID(
          `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(riders.country_code, contracts.city_id),
            riders.country_code
        ) AS lg_city_uuid,
        contracts.type,
        contracts.termination_type,
        contracts.status,
        contracts.name,
        contracts.job_title,
        contracts.start_at AS start_at_utc,
        contracts.end_at AS end_at_utc,
        contracts.termination_reason,
        contracts.created_at AS created_at_utc,
        contracts.updated_at AS updated_at_utc,
        contracts_agg_interval_rules.interval_rules
      )
    ) AS contracts
  FROM `fulfillment-dwh-production.curated_data_shared.riders` AS riders
  CROSS JOIN UNNEST (riders.contracts) AS contracts
  LEFT JOIN contracts_agg_interval_rules
         ON riders.country_code = contracts_agg_interval_rules.country_code
        AND riders.rider_id = contracts_agg_interval_rules.rider_id
        AND contracts.id = contracts_agg_interval_rules.contract_id
  GROUP BY
    riders.country_code,
    riders.rider_id
)

SELECT
  riders.rider_id AS id,
  `{project_id}`.pandata_intermediate.LG_UUID(riders.rider_id, riders.country_code) AS uuid,
  riders.reporting_to AS reporting_to_lg_rider_id,
  `{project_id}`.pandata_intermediate.LG_UUID(riders.reporting_to, riders.country_code) AS reporting_to_lg_rider_uuid,

  `{project_id}`.pandata_intermediate.EXCLUDE_T2(riders.country_code) AS country_code,
  riders.rider_name AS name,
  riders.email,
  riders.phone_number,
  riders.batch_number,
  riders.birth_date,
  riders.created_at AS created_at_utc,
  riders.updated_at AS updated_at_utc,
  riders_agg_contracts.contracts,
  riders_agg_batches.batches,
  riders.custom_fields,
  riders_agg_absences_history.absences_history,
  riders_agg_vehicles.vehicles,
  riders_agg_starting_points.starting_points,
  riders_agg_feedbacks.feedbacks,
FROM `fulfillment-dwh-production.curated_data_shared.riders` AS riders
LEFT JOIN riders_agg_contracts
       ON riders.country_code = riders_agg_contracts.country_code
      AND riders.rider_id = riders_agg_contracts.rider_id
LEFT JOIN riders_agg_batches
       ON riders.country_code = riders_agg_batches.country_code
      AND riders.rider_id = riders_agg_batches.rider_id
LEFT JOIN riders_agg_absences_history
       ON riders.country_code = riders_agg_absences_history.country_code
      AND riders.rider_id = riders_agg_absences_history.rider_id
LEFT JOIN riders_agg_vehicles
       ON riders.country_code = riders_agg_vehicles.country_code
      AND riders.rider_id = riders_agg_vehicles.rider_id
LEFT JOIN riders_agg_starting_points
       ON riders.country_code = riders_agg_starting_points.country_code
      AND riders.rider_id = riders_agg_starting_points.rider_id
LEFT JOIN riders_agg_feedbacks
       ON riders.country_code = riders_agg_feedbacks.country_code
      AND riders.rider_id = riders_agg_feedbacks.rider_id
