SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(vendor_id, rdbms_id) AS vendor_uuid,
  vendor_id,

  rider_payment_handling AS rider_payment_handling_type,
  rider_payout AS rider_payout_type,
  vendor_prepayment AS vendor_prepayment_type,
  ROW_NUMBER() OVER (PARTITION BY rdbms_id, vendor_id ORDER BY id DESC) = 1 AS is_latest,
  CAST(minimum_order_value_diff_charge AS BOOLEAN) AS is_minimum_order_value_difference_charged,
  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
  dwh_last_modified AS dwh_last_modified_at_utc,
FROM `{project_id}.pandata_raw_ml_backend_latest.vendor_configuration`
