WITH rider_wallet_transactions_agg_issues AS (
  SELECT
    rider_wallet_transactions.transaction_id,
    rider_wallet_transactions.country_code,
    ARRAY_AGG(
      STRUCT(
        issues.id,
        issues.original_amount AS original_amount_local,
        issues.justification,
        issues.reviewed AS is_reviewed,
        issues.approved AS is_approved,
        issues.created_by,
        issues.updated_by,
        issues.created_at AS created_at_utc,
        issues.updated_at AS updated_at_utc
      )
    ) AS issues
  FROM `fulfillment-dwh-production.curated_data_shared.rider_wallet` AS rider_wallet_transactions
  CROSS JOIN UNNEST (rider_wallet_transactions.issues) AS issues
  GROUP BY
    rider_wallet_transactions.transaction_id,
    rider_wallet_transactions.country_code
)

SELECT
  rider_wallet_transactions.transaction_id AS uuid,
  rider_wallet_transactions.transaction_id AS id,
  `{project_id}`.pandata_intermediate.LG_UUID(rider_wallet_transactions.rider_id, rider_wallet_transactions.country_code) AS lg_rider_uuid,
  rider_wallet_transactions.rider_id AS lg_rider_id,
  `{project_id}`.pandata_intermediate.LG_UUID(rider_wallet_transactions.delivery_id, rider_wallet_transactions.country_code) AS lg_delivery_uuid,
  rider_wallet_transactions.delivery_id AS lg_delivery_id,
  `{project_id}`.pandata_intermediate.LG_UUID(rider_wallet_transactions.order_id, rider_wallet_transactions.country_code) AS lg_order_uuid,
  rider_wallet_transactions.order_id AS lg_order_id,
  `{project_id}`.pandata_intermediate.LG_UUID(
    `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(rider_wallet_transactions.country_code, rider_wallet_transactions.city_id),
    rider_wallet_transactions.country_code
  ) AS lg_city_uuid,
  `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(rider_wallet_transactions.country_code, rider_wallet_transactions.city_id) AS lg_city_id,
  rider_wallet_transactions.external_provider_id,
  rider_wallet_transactions.integrator_id,
  `{project_id}`.pandata_intermediate.EXCLUDE_T2(rider_wallet_transactions.country_code) AS country_code,

  rider_wallet_transactions.type,
  rider_wallet_transactions.amount AS amount_local,
  rider_wallet_transactions.balance AS balance_local,
  rider_wallet_transactions.created_by,
  rider_wallet_transactions.note,

  rider_wallet_transactions.manual AS is_manual,
  rider_wallet_transactions.mass_update AS is_mass_update,

  rider_wallet_transactions.timezone,
  rider_wallet_transactions.created_date AS created_date_utc,
  rider_wallet_transactions.created_at AS created_at_utc,
  rider_wallet_transactions.updated_at AS updated_at_utc,
  rider_wallet_transactions_agg_issues.issues,
FROM `fulfillment-dwh-production.curated_data_shared.rider_wallet` AS rider_wallet_transactions
LEFT JOIN rider_wallet_transactions_agg_issues
       ON rider_wallet_transactions.country_code = rider_wallet_transactions_agg_issues.country_code
      AND rider_wallet_transactions.transaction_id = rider_wallet_transactions_agg_issues.transaction_id
