SELECT
  rider_wallet_transactions.uuid,
  rider_wallet_transactions.id,
  countries.rdbms_id,
  rider_wallet_transactions.lg_rider_uuid,
  rider_wallet_transactions.lg_rider_id,
  rider_wallet_transactions.lg_delivery_uuid,
  rider_wallet_transactions.lg_delivery_id,
  rider_wallet_transactions.lg_order_uuid,
  rider_wallet_transactions.lg_order_id,
  rider_wallet_transactions.lg_city_uuid,
  rider_wallet_transactions.lg_city_id,
  rider_wallet_transactions.external_provider_id,
  rider_wallet_transactions.integrator_id,
  rider_wallet_transactions.country_code,
  rider_wallet_transactions.type,
  rider_wallet_transactions.amount_local,
  rider_wallet_transactions.balance_local,
  rider_wallet_transactions.created_by,
  rider_wallet_transactions.note,
  rider_wallet_transactions.is_manual,
  rider_wallet_transactions.is_mass_update,
  rider_wallet_transactions.timezone,
  rider_wallet_transactions.created_date_utc,
  rider_wallet_transactions.created_at_utc,
  rider_wallet_transactions.updated_at_utc,
  rider_wallet_transactions.issues,
FROM `{project_id}.pandata_intermediate.lg_rider_wallet_transactions` AS rider_wallet_transactions
LEFT JOIN `{project_id}.pandata_intermediate.pd_countries` AS countries
       ON rider_wallet_transactions.country_code = countries.lg_country_code
