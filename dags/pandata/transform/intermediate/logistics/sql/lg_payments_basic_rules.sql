WITH payments_basic_rules_agg_applies_to AS (
  SELECT
    payments_basic_rules.country_code,
    payments_basic_rules.id AS payments_basic_rule_id,
    ARRAY_AGG(
      STRUCT(
        applies_to.rider_ids AS lg_rider_ids,
        applies_to.starting_point_ids AS lg_starting_point_ids,
        applies_to.city_ids AS lg_city_ids,
        applies_to.contract_ids AS lg_contract_ids,
        applies_to.days_of_week,
        applies_to.contract_types,
        applies_to.vehicle_types,
        applies_to.job_titles
      )
    ) AS applies_to,
  FROM `fulfillment-dwh-production.curated_data_shared.payments_basic_rules` AS payments_basic_rules
  CROSS JOIN UNNEST(payments_basic_rules.applies_to) AS applies_to
  GROUP BY
    payments_basic_rules.country_code,
    payments_basic_rules.id
)

SELECT
  `{project_id}`.pandata_intermediate.LG_UUID(payments_basic_rules.id, payments_basic_rules.country_code) AS uuid,
  payments_basic_rules.id,
  `{project_id}`.pandata_intermediate.EXCLUDE_T2(payments_basic_rules.country_code) AS country_code,
  payments_basic_rules.type,
  payments_basic_rules.sub_type,
  payments_basic_rules.name,
  payments_basic_rules.amount AS amount_local,
  payments_basic_rules.threshold AS min_threshold_in_km,
  payments_basic_rules.max_threshold AS max_threshold_in_km,
  NULLIF(payments_basic_rules.created_by, '123456789') AS created_by_email,
  payments_basic_rules.status = 'APPROVED' AS is_approved,
  payments_basic_rules.status,
  payments_basic_rules.active AS is_active,
  payments_basic_rules.is_pay_below_threshold,
  payments_basic_rules.acceptance_rate,
  payments_basic_rules.start_time,
  payments_basic_rules.end_time,
  payments_basic_rules.start_date AS start_date_utc,
  payments_basic_rules.end_date AS end_date_utc,
  payments_basic_rules.created_at AS created_at_utc,
  payments_basic_rules.updated_at AS updated_at_utc,
  payments_basic_rules_agg_applies_to.applies_to,
FROM `fulfillment-dwh-production.curated_data_shared.payments_basic_rules` AS payments_basic_rules
LEFT JOIN payments_basic_rules_agg_applies_to
       ON payments_basic_rules.country_code = payments_basic_rules_agg_applies_to.country_code
      AND payments_basic_rules.id = payments_basic_rules_agg_applies_to.payments_basic_rule_id
