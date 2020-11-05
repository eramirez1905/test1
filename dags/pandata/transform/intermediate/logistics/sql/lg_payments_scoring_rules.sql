WITH payments_scoring_rules_agg_cost_factors AS (
  SELECT
    payments_scoring_rules.id AS payments_scoring_rule_id,
    payments_scoring_rules.country_code,
    ARRAY_AGG(
      STRUCT(
        cost_factors.created_by AS created_by_email,
        cost_factors.threshold AS minimum_threshold,
        cost_factors.type,
        cost_factors.amount,
        cost_factors.created_at AS created_at_utc,
        cost_factors.updated_at AS updated_at_utc
      )
    ) AS cost_factors,
  FROM `fulfillment-dwh-production.curated_data_shared.payments_scoring_rules` AS payments_scoring_rules
  CROSS JOIN UNNEST (payments_scoring_rules.cost_factors) AS cost_factors
  GROUP BY
    payments_scoring_rules.id,
    payments_scoring_rules.country_code
),

payments_scoring_rules_agg_applies_to AS (
  SELECT
    payments_scoring_rules.id AS payments_scoring_rule_id,
    payments_scoring_rules.country_code,
    ARRAY_AGG(
      STRUCT(
        applies_to.contract_ids AS lg_contract_ids,
        applies_to.rider_ids AS lg_rider_ids,
        applies_to.starting_point_ids AS lg_starting_point_ids,
        `{project_id}`.pandata_intermediate.LG_UUID(
          `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(payments_scoring_rules.country_code, applies_to.city_id),
          payments_scoring_rules.country_code
        ) AS lg_city_uuid,
        `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(payments_scoring_rules.country_code, applies_to.city_id) AS lg_city_id,

        applies_to.vertical_types,
        applies_to.vehicle_types,
        applies_to.contract_types,
        applies_to.job_titles
      )
    ) AS applies_to,
  FROM `fulfillment-dwh-production.curated_data_shared.payments_scoring_rules` AS payments_scoring_rules
  CROSS JOIN UNNEST (payments_scoring_rules.applies_to) AS applies_to
  GROUP BY
    payments_scoring_rules.id,
    payments_scoring_rules.country_code
)

SELECT
  `{project_id}`.pandata_intermediate.LG_UUID(payments_scoring_rules.id, payments_scoring_rules.country_code) AS uuid,
  payments_scoring_rules.id,
  `{project_id}`.pandata_intermediate.LG_UUID(
    `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(payments_scoring_rules.country_code, payments_scoring_rules.city_id),
    payments_scoring_rules.country_code
  ) AS lg_city_uuid,
  `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(payments_scoring_rules.country_code, payments_scoring_rules.city_id) AS lg_city_id,

  `{project_id}`.pandata_intermediate.EXCLUDE_T2(payments_scoring_rules.country_code) AS country_code,
  payments_scoring_rules.created_by AS created_by_email,
  payments_scoring_rules.status,
  payments_scoring_rules.type,
  payments_scoring_rules.name,

  payments_scoring_rules.paid_unit AS payment_unit,
  payments_scoring_rules.paid_unit_type AS payment_unit_type,

  payments_scoring_rules.active AS is_active,

  payments_scoring_rules.start_date AS start_at_utc,
  payments_scoring_rules.end_date AS end_at_utc,
  payments_scoring_rules.created_at AS created_at_utc,
  payments_scoring_rules.updated_at AS updated_at_utc,
  payments_scoring_rules.created_date AS created_date_utc,
  payments_scoring_rules_agg_applies_to.applies_to,
  payments_scoring_rules_agg_cost_factors.cost_factors,
FROM `fulfillment-dwh-production.curated_data_shared.payments_scoring_rules` AS payments_scoring_rules
LEFT JOIN payments_scoring_rules_agg_applies_to
       ON payments_scoring_rules.country_code = payments_scoring_rules_agg_applies_to.country_code
      AND payments_scoring_rules.id = payments_scoring_rules_agg_applies_to.payments_scoring_rule_id
LEFT JOIN payments_scoring_rules_agg_cost_factors
       ON payments_scoring_rules.country_code = payments_scoring_rules_agg_cost_factors.country_code
      AND payments_scoring_rules.id = payments_scoring_rules_agg_cost_factors.payments_scoring_rule_id
