CREATE TEMP FUNCTION TIMESTAMP_DIFF_SECOND(later_timestamp TIMESTAMP, earlier_timestamp TIMESTAMP) AS (
  IF(
    later_timestamp > earlier_timestamp,
    TIMESTAMP_DIFF(later_timestamp, earlier_timestamp, SECOND),
    NULL
  )
);

-- attempt to fix timestamps that are not formatted properly e.g., YYYY-MM-DD HH:MM, otherwise NULL
CREATE TEMP FUNCTION CLEAN_TIMESTAMP(ts_str STRING) AS (
  CASE
    WHEN TRIM(ts_str) = 'undefined' OR TRIM(ts_str) = 'null' THEN NULL
    WHEN REGEXP_CONTAINS(TRIM(ts_str), r'^\d{{4}}-\d{{2}}-\d{{2}} \d+:\d{{2}}$') THEN CONCAT(TRIM(ts_str), ":00")
    ELSE TRIM(ts_str)
  END
);

WITH orders_agg_payments AS (
  SELECT
    rdbms_id,
    order_code,
    ARRAY_AGG(
      STRUCT(
        uuid,
        id,
        amount_local,
        payment_method_type,
        created_at_utc,
        updated_at_utc,
        dwh_last_modified_at_utc
      )
    ) AS payments
  FROM `{project_id}.pandata_intermediate.pd_order_payments` AS order_payments
  GROUP BY
    rdbms_id,
    order_code
),

order_products_agg_order_toppings AS (
  SELECT
    order_product_uuid,
    ARRAY_AGG(
      STRUCT(
        title,
        half_position,
        price_local,
        created_at_utc,
        updated_at_utc,
        dwh_last_modified_at_utc
      )
    ) AS toppings
  FROM `{project_id}.pandata_intermediate.pd_order_toppings`
  GROUP BY order_product_uuid
),

orders_agg_order_products AS (
  SELECT
    order_products.order_uuid,
    ARRAY_AGG(
      STRUCT(
        order_products.title,
        order_products.description,
        order_products.variation_title,
        order_products.quantity,
        order_products.sold_out_option,
        order_products.special_instructions,
        order_products.is_half,
        order_products.vat_rate,
        order_products.container_price_local,
        order_products.discount_local,
        order_products.price_local,
        order_products.sponsorship_local,
        order_products.toppings_half_price_local,
        order_products.toppings_price_local,
        order_products.total_price_local,
        order_products.total_vat_amount_local,
        order_products.created_at_utc,
        order_products.updated_at_utc,
        order_products.dwh_last_modified_at_utc,
        order_products_agg_order_toppings.toppings
      )
    ) AS products
  FROM `{project_id}.pandata_intermediate.pd_order_products` AS order_products
  LEFT JOIN order_products_agg_order_toppings
         ON order_products.uuid = order_products_agg_order_toppings.order_product_uuid
  GROUP BY order_uuid
),

orders_agg_calls AS (
  SELECT
    order_uuid,
    ARRAY_AGG(
      STRUCT(
        created_by_user_id,
        updated_by_user_id,
        is_active,
        attempt_number,
        api_used,
        fail_reason,
        phone_number,
        executed_at_local,
        scheduled_for_at_local,
        created_at_local,
        updated_at_local,
        dwh_last_modified_at_utc
      )
    ) AS calls
  FROM `{project_id}.pandata_intermediate.pd_calls`
  GROUP BY order_uuid
),

orders_agg_assignment_flows AS (
  SELECT
    order_assignment_flows.order_uuid,
    MIN(IF(users.has_vendor_role, order_assignment_flows.created_at_local, NULL)) AS idle_end_at_local,
    ARRAY_AGG(
      STRUCT(
        users.uuid,
        users.id,
        users.first_name,
        users.last_name,
        users.has_vendor_role,
        users.is_automated
      )
    ) AS users
  FROM `{project_id}.pandata_intermediate.pd_order_assignment_flows` AS order_assignment_flows
  LEFT JOIN `{project_id}.pandata_intermediate.pd_users` AS users
         ON order_assignment_flows.user_uuid = users.uuid
  GROUP BY order_assignment_flows.order_uuid
),

orders_agg_status_flows AS (
  SELECT
    pd_status_flows.order_uuid,
    MIN(IF(pd_status.is_customer_verification_start, pd_status_flows.created_at_local, NULL)) AS customer_verification_start_at_local,
    MIN(IF(pd_status.is_customer_verification_end, pd_status_flows.created_at_local, NULL)) AS customer_verification_end_at_local,
    MIN(IF(pd_status.is_credit_card_handling_end, pd_status_flows.created_at_local, NULL)) AS cc_handling_end_at_local,
    MIN(IF(pd_status.is_dispatcher_start, pd_status_flows.created_at_local, NULL)) AS dispatcher_start_at_local,
    MIN(IF(pd_status.is_dispatcher_end, pd_status_flows.created_at_local, NULL)) AS dispatcher_end_at_local,
    MIN(IF(pd_status.is_order_placement_start, pd_status_flows.created_at_local, NULL)) AS order_placement_start_at_local,
    MIN(IF(pd_status.is_order_placement_end, pd_status_flows.created_at_local, NULL)) AS order_placement_end_at_local,
    MIN(IF(pd_status.is_idle_start, pd_status_flows.created_at_local, NULL)) AS idle_start_at_local,
    MIN(IF(pd_status.is_online_payment_process_start, pd_status_flows.created_at_local, NULL)) AS online_payment_process_start_at_local,
    MIN(IF(pd_status.is_online_payment_process_end, pd_status_flows.created_at_local, NULL)) AS online_payment_process_end_at_local,
    MIN(IF(pd_status.is_completed_order, pd_status_flows.created_at_local, NULL)) AS order_completed_at_local,
    MIN(IF(pd_status.is_failed_order_vendor, pd_status_flows.created_at_local, NULL)) AS cancelled_by_vendor_at_local,
    MIN(IF(pd_status.is_cancelled_by_foodpanda, pd_status_flows.created_at_local, NULL)) AS cancelled_by_foodpanda_at_local,
    MIN(IF(pd_status.is_failed_order_customer, pd_status_flows.created_at_local, NULL)) AS cancelled_by_customer_at_local,
    IF(
      (
        (
          LOGICAL_OR(pd_status.is_open_call_vendor)
          AND LOGICAL_AND(
            pd_status_flows.is_earliest_status_flow
            AND pd_users.has_vendor_role
          )
          AND LOGICAL_OR(pd_status.is_cancelled_automatically)
        ) OR (
          LOGICAL_OR(pd_status.is_open_auto_inform_vendor)
          AND LOGICAL_OR(pd_status.is_error_call_vendor_or_customer)
        )
      ) AND NOT (
          LOGICAL_OR(pd_status.is_open_call_vendor)
          AND LOGICAL_OR(pd_status.is_open_auto_inform_vendor)
          AND LOGICAL_OR(pd_status.is_error_call_vendor_or_customer)
          AND LOGICAL_AND(
            pd_status_flows.is_earliest_status_flow
            AND pd_users.has_vendor_role
          )
          AND LOGICAL_OR(pd_status.is_cancelled_automatically)
      ),
      MIN(pd_status_flows.created_at_utc),
      NULL
    ) AS cc_handling_start_at_local,

    IF(
      LOGICAL_OR(pd_status.is_vendor_informed_wait_for_confirmation)
      AND (
        (
          LOGICAL_OR(pd_status.is_open_call_vendor)
          AND LOGICAL_AND(
            pd_status_flows.is_earliest_status_flow
            AND pd_users.has_vendor_role
          )
          AND LOGICAL_OR(pd_status.is_cancelled_automatically)
        )
        OR LOGICAL_OR(pd_status.is_open_auto_inform_vendor)
      ),
      MIN(pd_status_flows.created_at_utc),
      NULL
    ) AS vendor_confirmation_start_at_local,

    NOT (
      LOGICAL_OR(pd_status.is_cancelled_automatically)
      OR (
        LOGICAL_OR(pd_status.is_not_automated)
        AND NOT LOGICAL_AND(pd_users.is_automated)
      )
    ) AS is_automated,

    ARRAY_AGG(
      STRUCT(
        pd_status_flows.uuid,
        pd_status_flows.id,
        pd_status.code,
        pd_status.title,
        pd_status_flows.payment_status_type,
        pd_status_flows.is_earliest_status_flow,
        pd_status_flows.is_payment_status_pending,
        pd_status_flows.is_payment_status_fraud,
        pd_status_flows.is_payment_status_in_progress,
        pd_status_flows.is_payment_status_canceled_to_wallet,
        pd_status_flows.is_payment_status_refused,
        pd_status_flows.is_payment_status_refund,
        pd_status_flows.is_payment_status_cash_on_delivery,
        pd_status_flows.is_payment_status_payed,
        pd_status_flows.is_payment_status_error,
        pd_status_flows.is_payment_status_soft_paid,
        pd_status_flows.is_payment_status_canceled,
        pd_status_flows.is_payment_status_paid,
        pd_status.is_cart_editable,
        pd_status.is_deprecated,
        pd_status.is_traffic_manager_declined,
        pd_status.is_valid_order,
        pd_status.is_test_order,
        pd_status.is_final_order_state,
        pd_status.is_failed_order,
        pd_status.is_failed_order_vendor,
        pd_status.is_failed_order_customer,
        pd_status.is_failed_order_foodpanda,
        pd_status.is_cancelled_by_foodpanda,
        pd_status.is_cancelled_automatically,
        pd_status.is_not_automated,
        pd_status.is_replaced_order,
        pd_status.is_incompleted_order,
        pd_status.is_completed_order,
        pd_status.is_gross_order,
        pd_status.is_idle_start,
        pd_status.is_online_payment_process_start,
        pd_status.is_online_payment_process_end,
        pd_status.is_customer_verification_start,
        pd_status.is_customer_verification_end,
        pd_status.is_credit_card_handling_start,
        pd_status.is_credit_card_handling_end,
        pd_status.is_dispatcher_start,
        pd_status.is_dispatcher_end,
        pd_status.is_order_placement_start,
        pd_status.is_order_placement_end,
        pd_status_flows.created_at_local,
        pd_status_flows.created_at_utc
      )
    ) AS status_flows
  FROM `{project_id}.pandata_intermediate.pd_status_flows` AS pd_status_flows
  LEFT JOIN `{project_id}.pandata_intermediate.pd_status` AS pd_status
         ON pd_status_flows.status_uuid = pd_status.uuid
  LEFT JOIN `{project_id}.pandata_intermediate.pd_users` AS pd_users
         ON pd_status_flows.user_uuid = pd_users.uuid
  GROUP BY pd_status_flows.order_uuid
)

SELECT
  orders.uuid,
  orders.id,
  orders.rdbms_id,
  orders.assignee_id,
  orders.calculation_configuration_id,
  orders.customer_id,
  orders.code,
  orders.decline_reason_id,
  orders.delivery_area_id,
  orders.payment_type_id,
  orders.status_id,
  orders.user_id,
  orders.vendor_id,
  orders.commission,
  orders.commission_type,
  orders.customer_address_comment,
  orders.customer_comment,
  orders.order_comment,
  orders.delivery_address_city,
  orders.delivery_address_company,
  orders.delivery_address_id,
  orders.delivery_address,
  orders.delivery_address_number,
  orders.delivery_address_other,
  orders.delivery_address_postcode,
  orders.delivery_address_flat_number,
  orders.delivery_address_floor_number,
  orders.delivery_address_room,
  orders.delivery_address_structure,
  orders.delivery_building,
  orders.delivery_company,
  orders.delivery_district,
  orders.delivery_entrance,
  orders.expense_code,
  orders.vendor_daily_order_number,
  orders.billable_for_vendor_code,
  orders.free_gift,
  orders.gmv_modifier,
  orders.payment_attempts,
  orders.payment_status,
  orders.pickup_location,
  orders.platform,
  orders.difference_to_minimum_vat_rate,
  orders.delivery_fee_vat_rate,
  orders.source,
  orders.vendor_comment,
  orders.vat_rate,
  orders.is_preorder,
  orders.is_delivery_address_verified,
  orders.is_express_delivery,
  orders.is_feedback_sent,
  orders.is_fraud_level,
  orders.is_archived,
  orders.is_edited,
  orders.is_delivery,
  orders.is_vendor_accepted,
  orders.is_pickup,
  orders_agg_status_flows.is_automated,
  orders.has_email_feedback,
  orders.has_delivery_charge,
  orders.has_service_charge,
  orders.allowance_amount_local,
  orders.container_price_local,
  orders.calculated_total_local,
  orders.cash_change_local,
  orders.charity_local,
  orders.vendor_prepayment_local,
  orders.payable_total_local,
  orders.difference_to_minimum_local,
  orders.difference_to_minimum_plus_vat_local,
  orders.minimum_delivery_value_local,
  orders.delivery_fee_local,
  orders.delivery_fee_forced_local,
  orders.delivery_fee_original_local,
  orders.delivery_fee_vat_local,
  orders.vendor_delivery_fee_local,
  orders.products_subtotal_local,
  orders.products_total_local,
  orders.products_vat_amount_local,
  orders.rider_tip_local,
  orders.service_fee_local,
  orders.service_fee_total_local,
  orders.service_tax_local,
  orders.service_tax_total_local,
  orders.subtotal_local,
  orders.total_value_local,
  orders.vat_amount_local,
  orders.automatic_delay_in_minutes,
  orders.cooking_time_in_minutes,
  orders.delivery_time_in_minutes,
  orders.preparation_buffer_in_minutes,
  orders.preparation_time_in_minutes,
  orders.promised_delivery_time_in_minutes,
  TIMESTAMP_DIFF_SECOND(orders_agg_assignment_flows.idle_end_at_local, orders_agg_status_flows.idle_start_at_local) AS idle_time_in_seconds,
  TIMESTAMP_DIFF_SECOND(orders_agg_status_flows.online_payment_process_end_at_local, orders_agg_status_flows.online_payment_process_start_at_local) AS online_payment_time_in_seconds,
  TIMESTAMP_DIFF_SECOND(orders_agg_status_flows.customer_verification_end_at_local, orders_agg_status_flows.customer_verification_start_at_local) AS customer_verification_time_in_seconds,
  TIMESTAMP_DIFF_SECOND(orders_agg_status_flows.cc_handling_end_at_local, orders_agg_status_flows.cc_handling_start_at_local) AS cc_handling_time_in_seconds,
  TIMESTAMP_DIFF_SECOND(orders_agg_status_flows.customer_verification_end_at_local, orders_agg_status_flows.vendor_confirmation_start_at_local) AS vendor_confirmation_time_in_seconds,
  TIMESTAMP_DIFF_SECOND(orders_agg_status_flows.dispatcher_end_at_local, orders_agg_status_flows.dispatcher_start_at_local) AS dispatcher_time_in_seconds,
  TIMESTAMP_DIFF_SECOND(orders_agg_status_flows.order_placement_end_at_local, orders_agg_status_flows.order_placement_start_at_local) AS order_placement_time_in_seconds,
  orders.rider_pickup_at_local,
  SAFE_CAST(CLEAN_TIMESTAMP(orders.expected_delivery_at_local) AS TIMESTAMP) AS expected_delivery_at_local,
  SAFE_CAST(CLEAN_TIMESTAMP(orders.promised_expected_delivery_at_local) AS TIMESTAMP) AS promised_expected_delivery_at_local,
  orders_agg_status_flows.customer_verification_start_at_local,
  orders_agg_status_flows.customer_verification_end_at_local,
  orders_agg_status_flows.cc_handling_start_at_local,
  orders_agg_status_flows.cc_handling_end_at_local,
  orders_agg_status_flows.dispatcher_start_at_local,
  orders_agg_status_flows.dispatcher_end_at_local,
  orders_agg_status_flows.order_placement_start_at_local,
  orders_agg_status_flows.order_placement_end_at_local,
  orders_agg_status_flows.idle_start_at_local,
  orders_agg_assignment_flows.idle_end_at_local,
  orders_agg_status_flows.online_payment_process_start_at_local,
  orders_agg_status_flows.online_payment_process_end_at_local,
  orders_agg_status_flows.order_completed_at_local,
  orders_agg_status_flows.cancelled_by_vendor_at_local,
  orders_agg_status_flows.cancelled_by_foodpanda_at_local,
  orders_agg_status_flows.cancelled_by_customer_at_local,
  orders_agg_status_flows.vendor_confirmation_start_at_local,
  orders.ordered_at_local,
  orders.status_updated_at_utc,
  orders.ordered_at_date_local,
  orders.assigned_at_utc,
  orders.created_at_utc,
  DATE(orders.created_at_utc) AS created_date_utc,
  orders.updated_at_utc,
  orders.dwh_last_modified_at_utc,
  STRUCT(
    delivery_providers.title,
    delivery_providers.description,
    delivery_providers.email,
    delivery_providers.fax,
    delivery_providers.mobile,
    delivery_providers.phone,
    delivery_providers.preferred_contact,
    delivery_providers.third_party,
    delivery_providers.is_active,
    delivery_providers.is_type_own_delivery_foodpanda,
    delivery_providers.is_type_own_delivery_third_party,
    delivery_providers.is_type_vendor_delivery_third_party,
    delivery_providers.has_express_delivery,
    delivery_providers.type
  ) AS delivery_provider,
  orders_agg_order_products.products,
  orders_agg_calls.calls,
  orders_agg_status_flows.status_flows,
  orders_agg_payments.payments,
FROM `{project_id}.pandata_intermediate.pd_orders` AS orders
LEFT JOIN orders_agg_order_products
       ON orders.uuid = orders_agg_order_products.order_uuid
LEFT JOIN orders_agg_calls
       ON orders.uuid = orders_agg_calls.order_uuid
LEFT JOIN orders_agg_status_flows
       ON orders.uuid = orders_agg_status_flows.order_uuid
LEFT JOIN orders_agg_assignment_flows
       ON orders.uuid = orders_agg_assignment_flows.order_uuid
LEFT JOIN `{project_id}.pandata_intermediate.pd_delivery_providers` AS delivery_providers
       ON orders.delivery_provider_uuid = delivery_providers.uuid
LEFT JOIN orders_agg_payments
       ON orders.rdbms_id = orders_agg_payments.rdbms_id
      AND orders.code = orders_agg_payments.order_code
