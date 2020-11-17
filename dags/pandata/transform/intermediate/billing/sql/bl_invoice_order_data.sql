SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(invoice_id, rdbms_id) AS invoice_uuid,
  invoice_id,

  order_code AS pd_order_code,
  delivery_provider_type AS pd_delivery_provider_type,
  order_status AS pd_order_status_code,
  payment_type,

  invoice_number,
  invoice_tax,

  already_received_amount AS already_received_amount_local,
  balance AS balance_local,
  fp_discount AS foodpanda_discount,
  vendor_discount AS vendor_discount_local,
  service_tax AS service_tax_local,
  vat AS vat_local,
  online_payment_charge AS online_payment_charge_local,
  container_charge AS container_charge_local,
  delivery_fee AS delivery_fee_local,
  service_fee AS service_fee_local,
  restaurant_revenue AS restaurant_revenue_local,
  total_commission AS total_commission_local,
  total_order AS total_price_local,
  products AS product_price_local,

  order_date AS ordered_at_utc,
  created_at AS created_at_utc,
  dwh_last_modified AS dwh_last_modified_utc,
FROM `{project_id}.pandata_raw_ml_billing_latest.invoice_order_data`
