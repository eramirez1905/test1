WITH payments_quest_rules_agg_cost_factors AS (
  SELECT
    payments_quest_rules.id AS payments_quest_rule_id,
    payments_quest_rules.country_code,
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
  FROM `fulfillment-dwh-production.curated_data_shared.payments_quest_rules` AS payments_quest_rules
  CROSS JOIN UNNEST (payments_quest_rules.cost_factors) AS cost_factors
  GROUP BY
    payments_quest_rules.id,
    payments_quest_rules.country_code
),

payments_quest_rules_agg_applies_to AS (
  SELECT
    payments_quest_rules.id AS payments_quest_rule_id,
    payments_quest_rules.country_code,
    ARRAY_AGG(
      STRUCT(
        applies_to.contract_ids AS lg_contract_ids,
        applies_to.rider_ids AS lg_rider_ids,
        applies_to.starting_point_ids AS lg_starting_point_ids,
        applies_to.city_ids AS lg_city_ids,

        applies_to.vertical_types,
        applies_to.vehicle_types,
        applies_to.contract_types,
        applies_to.job_titles,
        applies_to.days_of_week
      )
    ) AS applies_to,
  FROM `fulfillment-dwh-production.curated_data_shared.payments_quest_rules` AS payments_quest_rules
  CROSS JOIN UNNEST (payments_quest_rules.applies_to) AS applies_to
  GROUP BY
    payments_quest_rules.id,
    payments_quest_rules.country_code
)

SELECT
  `{project_id}`.pandata_intermediate.LG_UUID(payments_quest_rules.id, payments_quest_rules.country_code) AS uuid,
  payments_quest_rules.id,

  `{project_id}`.pandata_intermediate.EXCLUDE_T2(payments_quest_rules.country_code) AS country_code,
  payments_quest_rules.created_by AS created_by_email,
  payments_quest_rules.status,
  payments_quest_rules.type,
  payments_quest_rules.sub_type,
  payments_quest_rules.name,
  payments_quest_rules.duration AS duration_iso8601,

  payments_quest_rules.active AS is_active,
  payments_quest_rules.is_pay_below_threshold,
  payments_quest_rules.is_negative,

  payments_quest_rules.acceptance_rate AS minimum_acceptance_rate,
  payments_quest_rules.no_show_limit,

  payments_quest_rules.start_time,
  payments_quest_rules.end_time,
  payments_quest_rules.start_date AS start_at_utc,
  payments_quest_rules.end_date AS end_at_utc,
  payments_quest_rules.created_at AS created_at_utc,
  payments_quest_rules.updated_at AS updated_at_utc,
  payments_quest_rules.created_date AS created_date_utc,
  payments_quest_rules_agg_applies_to.applies_to,
  payments_quest_rules_agg_cost_factors.cost_factors,
FROM `fulfillment-dwh-production.curated_data_shared.payments_quest_rules` AS payments_quest_rules
LEFT JOIN payments_quest_rules_agg_applies_to
       ON payments_quest_rules.country_code = payments_quest_rules_agg_applies_to.country_code
      AND payments_quest_rules.id = payments_quest_rules_agg_applies_to.payments_quest_rule_id
LEFT JOIN payments_quest_rules_agg_cost_factors
       ON payments_quest_rules.country_code = payments_quest_rules_agg_cost_factors.country_code
      AND payments_quest_rules.id = payments_quest_rules_agg_cost_factors.payments_quest_rule_id
