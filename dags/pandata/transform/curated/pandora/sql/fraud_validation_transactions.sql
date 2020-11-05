SELECT
  fraud_validation_transactions.uuid,
  fraud_validation_transactions.id,
  fraud_validation_transactions.rdbms_id,
  fraud_validation_transactions.score,
  fraud_validation_transactions.validator_code,
  fraud_validation_transactions.order_code,
  fraud_validation_transactions.is_success,
  fraud_validation_transactions.is_error,
  fraud_validation_transactions.is_bad,
  fraud_validation_transactions.transaction_reference,
  fraud_validation_transactions.handler_data,
  fraud_validation_transactions.created_at_local,
  TIMESTAMP(DATETIME(fraud_validation_transactions.created_at_local), countries.timezone) AS created_at_utc,
  fraud_validation_transactions.created_date_local,
  DATE(TIMESTAMP(DATETIME(fraud_validation_transactions.created_at_local), countries.timezone)) AS created_date_utc,
FROM `{project_id}.pandata_intermediate.pd_fraud_validation_transactions` AS fraud_validation_transactions
LEFT JOIN `{project_id}.pandata_intermediate.pd_countries` AS countries
       ON fraud_validation_transactions.rdbms_id = countries.rdbms_id
