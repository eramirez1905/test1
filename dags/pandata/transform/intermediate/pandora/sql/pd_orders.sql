SELECT
  `{project_id}`.pandata_intermediate.PD_UUID(id, rdbms_id) AS uuid,
  id,
  rdbms_id,
  `{project_id}`.pandata_intermediate.PD_UUID(assignee_id, rdbms_id) AS assignee_uuid,
  assignee_id,
  `{project_id}`.pandata_intermediate.PD_UUID(calculation_configuration_id, rdbms_id) AS calculation_configuration_uuid,
  calculation_configuration_id,
  `{project_id}`.pandata_intermediate.PD_UUID(customer_id, rdbms_id) AS customer_uuid,
  customer_id,
  `{project_id}`.pandata_intermediate.PD_UUID(decline_reason_id, rdbms_id) AS decline_reason_uuid,
  decline_reason_id,
  `{project_id}`.pandata_intermediate.PD_UUID(delivery_area_id, rdbms_id) AS delivery_area_uuid,
  delivery_area_id,
  `{project_id}`.pandata_intermediate.PD_UUID(deliveryprovider_id, rdbms_id) AS delivery_provider_uuid,
  deliveryprovider_id AS delivery_provider_id,
  `{project_id}`.pandata_intermediate.PD_UUID(paymenttype_id, rdbms_id) AS payment_type_uuid,
  paymenttype_id AS payment_type_id,
  `{project_id}`.pandata_intermediate.PD_UUID(status_id, rdbms_id) AS status_uuid,
  status_id,
  `{project_id}`.pandata_intermediate.PD_UUID(user_id, rdbms_id) AS user_uuid,
  user_id,
  `{project_id}`.pandata_intermediate.PD_UUID(vendor_id, rdbms_id) AS vendor_uuid,
  vendor_id,
  `{project_id}`.pandata_intermediate.PD_UUID(delivery_address_id, rdbms_id) AS delivery_address_uuid,
  delivery_address_id,

  code,
  commission,
  commission_type,
  customer_address_comment,
  customer_comment,
  order_comment,
  delivery_address_city,
  delivery_address_company,
  NULLIF(
    (
      IFNULL(delivery_address_line1, "") || IF(delivery_address_line1 IS NULL, "", "\n") ||
      IFNULL(delivery_address_line2, "") || IF(delivery_address_line2 IS NULL, "", "\n") ||
      IFNULL(delivery_address_line3, "") || IF(delivery_address_line3 IS NULL, "", "\n") ||
      IFNULL(delivery_address_line4, "") || IF(delivery_address_line4 IS NULL, "", "\n") ||
      IFNULL(delivery_address_line5, "")
    ),
  ""
  ) AS delivery_address,
  delivery_address_number,
  delivery_address_other,
  delivery_address_postcode,
  delivery_flat_number AS delivery_address_flat_number,
  delivery_floor AS delivery_address_floor_number,
  delivery_room AS delivery_address_room,
  delivery_structure AS delivery_address_structure,
  delivery_building,
  delivery_company,
  delivery_district,
  delivery_entrance,
  expense_code,
  number AS vendor_daily_order_number,
  is_billable_for_vendor AS billable_for_vendor_code,
  free_gift,
  gmv_modifier,
  payment_attempts,
  payment_status,
  pickup_location,
  platform,
  difference_to_minimum_vat_rate,
  delivery_fee_vat_rate,
  source,
  vendor_comment,
  vat / 100 AS vat_rate,

  -- booleans
  IFNULL(preorder = 1, FALSE) AS is_preorder,
  delivery_address_verified = 1 AS is_delivery_address_verified,
  express_delivery = 1 AS is_express_delivery,
  feedback_sent = 1 AS is_feedback_sent,
  fraud_level = "1" AS is_fraud_level,
  archive = 1 AS is_archived,
  IFNULL(edited = 1, FALSE) AS is_edited,
  IFNULL(expedition_type = 'delivery', TRUE) AS is_delivery,
  vendor_accepted = 1 AS is_vendor_accepted,
  expedition_type = 'pickup' AS is_pickup,
  email_feedback = 1 AS has_email_feedback,
  IFNULL(delivery_charge = 1, FALSE) AS has_delivery_charge,
  IFNULL(service_charge = 1, FALSE) AS has_service_charge,

  -- currency
  allowance_amount AS allowance_amount_local,
  container_price AS container_price_local,
  calculated_total AS calculated_total_local,
  change_for AS cash_change_local,
  charity AS charity_local,
  pay_restaurant AS vendor_prepayment_local,
  payable_total AS payable_total_local,
  difference_to_minimum AS difference_to_minimum_local,
  difference_to_minimum_plus_vat AS difference_to_minimum_plus_vat_local,
  minimum_delivery_value AS minimum_delivery_value_local,
  IFNULL(delivery_fee, 0) AS delivery_fee_local,
  IFNULL(delivery_fee_forced, 0) AS delivery_fee_forced_local,
  IFNULL(delivery_fee_original, 0) AS delivery_fee_original_local,
  IFNULL(delivery_fee_vat, 0) AS delivery_fee_vat_local,
  vendor_delivery_fee AS vendor_delivery_fee_local,
  products_subtotal AS products_subtotal_local,
  products_total AS products_total_local,
  products_vat_amount AS products_vat_amount_local,
  rider_tip AS rider_tip_local,
  service_fee AS service_fee_local,
  service_fee_total AS service_fee_total_local,
  service_tax AS service_tax_local,
  service_tax_total AS service_tax_total_local,
  subtotal AS subtotal_local,
  total_value AS total_value_local,
  vat_amount AS vat_amount_local,

  -- time
  automatic_delay AS automatic_delay_in_minutes,
  cooking_time AS cooking_time_in_minutes,
  delivery_time AS delivery_time_in_minutes,
  preparation_buffer AS preparation_buffer_in_minutes,
  preparation_time AS preparation_time_in_minutes,
  promised_delivery_time AS promised_delivery_time_in_minutes,

  -- timestamps
  rider_pickup_datetime AS rider_pickup_at_local,
  expected_delivery_time AS expected_delivery_at_local, -- dirty data
  promised_expected_delivery_time AS promised_expected_delivery_at_local, -- dirty data
  date AS ordered_at_local,
  status_date AS status_updated_at_utc,
  DATE(date) AS ordered_at_date_local,
  assignee_date AS assigned_at_utc,
  created_at AS created_at_utc,
  updated_at AS updated_at_utc,
  dwh_last_modified AS dwh_last_modified_at_utc,
FROM `{project_id}.pandata_raw_ml_backend_latest.orders`