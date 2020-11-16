SELECT
  vouchers.uuid,
  vouchers.id,
  vouchers.rdbms_id,
  vouchers.customer_uuid,
  vouchers.customer_id,
  vouchers.user_uuid,
  vouchers.user_id,
  vouchers.payment_type_ids,
  vouchers.code,
  vouchers.customer_code,
  vouchers.quantity,
  vouchers.daily_usage_limit,
  vouchers.customer_quantity,
  vouchers.counter,
  vouchers.code_generation_strategy_type,
  vouchers.type,
  vouchers.product_category_voucher_unit_type,
  vouchers.value,
  vouchers.foodpanda_ratio,
  vouchers.purpose,
  vouchers.description,
  vouchers.channel,
  vouchers.voucher_not_valid_message,
  vouchers.is_general_use,
  vouchers.is_order_min_value_for_promotional_products,
  vouchers.is_new_customer,
  vouchers.is_new_customer_vertical,
  vouchers.is_auto_apply_enabled,
  vouchers.is_unlimited,
  vouchers.is_deleted,
  vouchers.is_type_third_party,
  vouchers.is_type_cashback_amount,
  vouchers.is_type_amount,
  vouchers.is_type_free_gift,
  vouchers.is_type_delivery_fee,
  vouchers.is_type_percentage,
  vouchers.is_type_conditional_bogo,
  vouchers.is_type_product_category,
  vouchers.is_type_joker_amount,
  vouchers.is_type_cashback_percentage,
  vouchers.minimum_order_value_local,
  vouchers.maximum_order_value_local,
  vouchers.maximum_discount_amount_local,
  vouchers.platforms,
  vouchers.expedition_types,
  vouchers.vertical_types,
  vouchers.start_date_local,
  vouchers.stop_date_local,
  vouchers.start_at_local,
  vouchers.stop_at_local,
  DATE(TIMESTAMP(DATETIME(vouchers.created_at_local), countries.timezone)) AS created_date_utc,
  vouchers.created_at_local,
  TIMESTAMP(DATETIME(vouchers.created_at_local), countries.timezone) AS created_at_utc,
  vouchers.updated_at_local,
  TIMESTAMP(DATETIME(vouchers.updated_at_local), countries.timezone) AS updated_at_utc,
  vouchers.dwh_last_modified_at_utc,
FROM `{project_id}.pandata_intermediate.pd_vouchers` AS vouchers
LEFT JOIN `{project_id}.pandata_intermediate.pd_countries` AS countries
       ON vouchers.rdbms_id = countries.rdbms_id