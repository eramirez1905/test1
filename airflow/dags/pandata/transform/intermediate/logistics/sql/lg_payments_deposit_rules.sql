WITH payments_depost_rules_agg_applies_to AS (
  SELECT
    payments_deposit_rules.country_code,
    payments_deposit_rules.id AS payments_deposit_rule_id,
    ARRAY_AGG(
      STRUCT(
        applies_to.contract_ids AS lg_contract_ids,
        applies_to.city_ids AS lg_city_ids
      )
    ) AS applies_to,
  FROM `fulfillment-dwh-production.curated_data_shared.payments_deposit_rules` AS payments_deposit_rules
  CROSS JOIN UNNEST(payments_deposit_rules.applies_to) AS applies_to
  GROUP BY
    payments_deposit_rules.country_code,
    payments_deposit_rules.id
)

SELECT
  `{project_id}`.pandata_intermediate.LG_UUID(payments_deposit_rules.id, payments_deposit_rules.country_code) AS uuid,
  payments_deposit_rules.id,
  `{project_id}`.pandata_intermediate.EXCLUDE_T2(payments_deposit_rules.country_code) AS country_code,
  payments_deposit_rules.region,
  payments_deposit_rules.name,
  payments_deposit_rules.description,
  payments_deposit_rules.cycle_number,
  payments_deposit_rules.gross_earning_percentage,
  payments_deposit_rules.status,
  payments_deposit_rules.amount AS amount_local,
  payments_deposit_rules.created_by AS created_by_email,
  payments_deposit_rules.approved_by AS approved_by_email,

  payments_deposit_rules.active AS is_active,

  payments_deposit_rules.created_at AS created_at_utc,
  payments_deposit_rules.updated_at AS updated_at_utc,
  payments_deposit_rules.approved_at AS approved_at_utc,
  payments_deposit_rules.created_date AS created_date_utc,
  payments_depost_rules_agg_applies_to.applies_to,
FROM `fulfillment-dwh-production.curated_data_shared.payments_deposit_rules` AS payments_deposit_rules
LEFT JOIN payments_depost_rules_agg_applies_to
       ON payments_deposit_rules.country_code = payments_depost_rules_agg_applies_to.country_code
      AND payments_deposit_rules.id = payments_depost_rules_agg_applies_to.payments_deposit_rule_id
