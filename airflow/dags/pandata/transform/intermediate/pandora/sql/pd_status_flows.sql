SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(order_id, rdbms_id) AS order_uuid,
  order_id,
  `{project_id}`.pandata_intermediate.PD_UUID(status_id, rdbms_id) AS status_uuid,
  status_id,
  `{project_id}`.pandata_intermediate.PD_UUID(user_id, rdbms_id) AS user_uuid,
  user_id,

  payment_status AS payment_status_type,
  payment_status = 'pending' AS is_payment_status_pending,
  payment_status = 'fraud' AS is_payment_status_fraud,
  payment_status = 'in_progress' AS is_payment_status_in_progress,
  payment_status = 'canceled_to_wallet' AS is_payment_status_canceled_to_wallet,
  payment_status = 'refused' AS is_payment_status_refused,
  payment_status = 'refund' AS is_payment_status_refund,
  payment_status = 'cash_on_delivery' AS is_payment_status_cash_on_delivery,
  payment_status = 'payed' AS is_payment_status_payed,
  payment_status = 'error' AS is_payment_status_error,
  payment_status = 'soft_paid' AS is_payment_status_soft_paid,
  payment_status = 'canceled' AS is_payment_status_canceled,
  payment_status = 'paid' AS is_payment_status_paid,

  date AS created_at_local,
  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
  RANK() OVER (
    PARTITION BY
      rdbms_id,
      order_id
    ORDER BY id
  ) = 1 AS is_earliest_status_flow,
  dwh_last_modified AS dwh_last_modified_utc,
FROM `{project_id}.pandata_raw_ml_backend_latest.statusflows`
