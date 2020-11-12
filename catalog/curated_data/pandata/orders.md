# orders

Table of orders with each row representing one order nested with products and toppings, calls

Partitioned by field `created_date_utc` with each
partition being a time interval of `DAY`

Clustered by field `rdbms_id`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid is unique and represents an order |
| id | `INTEGER` |  |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| assignee_id | `INTEGER` |  |
| calculation_configuration_id | `INTEGER` |  |
| customer_id | `INTEGER` |  |
| code | `STRING` |  |
| decline_reason_id | `INTEGER` |  |
| delivery_area_id | `INTEGER` |  |
| payment_type_id | `INTEGER` |  |
| status_id | `INTEGER` |  |
| user_id | `INTEGER` |  |
| vendor_id | `INTEGER` |  |
| commission | `FLOAT` |  |
| commission_type | `STRING` |  |
| customer_address_comment | `STRING` |  |
| customer_comment | `STRING` |  |
| order_comment | `STRING` |  |
| delivery_address_city | `STRING` |  |
| delivery_address_company | `STRING` |  |
| delivery_address_id | `INTEGER` |  |
| delivery_address | `STRING` |  |
| delivery_address_number | `STRING` |  |
| delivery_address_other | `STRING` |  |
| delivery_address_postcode | `STRING` |  |
| delivery_address_flat_number | `STRING` |  |
| delivery_address_floor_number | `STRING` |  |
| delivery_address_room | `STRING` |  |
| delivery_address_structure | `STRING` |  |
| delivery_building | `STRING` |  |
| delivery_company | `STRING` |  |
| delivery_district | `STRING` |  |
| delivery_entrance | `STRING` |  |
| expense_code | `STRING` |  |
| vendor_daily_order_number | `INTEGER` |  |
| billable_for_vendor_code | `INTEGER` |  |
| free_gift | `STRING` |  |
| gmv_modifier | `INTEGER` |  |
| payment_attempts | `INTEGER` |  |
| payment_status | `STRING` |  |
| pickup_location | `STRING` |  |
| platform | `STRING` |  |
| difference_to_minimum_vat_rate | `FLOAT` |  |
| delivery_fee_vat_rate | `FLOAT` |  |
| source | `STRING` |  |
| vendor_comment | `STRING` |  |
| vat_rate | `FLOAT` | Vat rate in percentage |
| is_preorder | `BOOLEAN` |  |
| is_delivery_address_verified | `BOOLEAN` |  |
| is_express_delivery | `BOOLEAN` |  |
| is_feedback_sent | `BOOLEAN` |  |
| is_fraud_level | `BOOLEAN` |  |
| is_archived | `BOOLEAN` |  |
| is_edited | `BOOLEAN` |  |
| is_delivery | `BOOLEAN` |  |
| is_vendor_accepted | `BOOLEAN` |  |
| is_pickup | `BOOLEAN` |  |
| is_automated | `BOOLEAN` |  |
| has_email_feedback | `BOOLEAN` |  |
| has_delivery_charge | `BOOLEAN` |  |
| has_service_charge | `BOOLEAN` |  |
| allowance_amount_local | `FLOAT` |  |
| container_price_local | `NUMERIC` |  |
| calculated_total_local | `FLOAT` |  |
| cash_change_local | `FLOAT` |  |
| charity_local | `FLOAT` |  |
| vendor_prepayment_local | `NUMERIC` |  |
| payable_total_local | `FLOAT` |  |
| difference_to_minimum_local | `FLOAT` |  |
| difference_to_minimum_plus_vat_local | `FLOAT` |  |
| minimum_delivery_value_local | `FLOAT` |  |
| delivery_fee_local | `FLOAT` |  |
| delivery_fee_forced_local | `FLOAT` |  |
| delivery_fee_original_local | `FLOAT` |  |
| delivery_fee_vat_local | `FLOAT` |  |
| vendor_delivery_fee_local | `FLOAT` |  |
| products_subtotal_local | `FLOAT` |  |
| products_total_local | `FLOAT` |  |
| products_vat_amount_local | `FLOAT` |  |
| rider_tip_local | `FLOAT` |  |
| service_fee_local | `FLOAT` |  |
| service_fee_total_local | `FLOAT` |  |
| service_tax_local | `FLOAT` |  |
| service_tax_total_local | `FLOAT` |  |
| subtotal_local | `FLOAT` |  |
| total_value_local | `FLOAT` |  |
| vat_amount_local | `FLOAT` |  |
| automatic_delay_in_minutes | `INTEGER` |  |
| cooking_time_in_minutes | `INTEGER` |  |
| delivery_time_in_minutes | `INTEGER` |  |
| preparation_buffer_in_minutes | `INTEGER` |  |
| preparation_time_in_minutes | `INTEGER` |  |
| promised_delivery_time_in_minutes | `INTEGER` |  |
| idle_time_in_seconds | `INTEGER` |  |
| online_payment_time_in_seconds | `INTEGER` |  |
| customer_verification_time_in_seconds | `INTEGER` |  |
| cc_handling_time_in_seconds | `INTEGER` |  |
| vendor_confirmation_time_in_seconds | `INTEGER` |  |
| dispatcher_time_in_seconds | `INTEGER` |  |
| order_placement_time_in_seconds | `INTEGER` |  |
| rider_pickup_at_local | `TIMESTAMP` |  |
| expected_delivery_at_local | `TIMESTAMP` |  |
| promised_expected_delivery_at_local | `TIMESTAMP` |  |
| customer_verification_start_at_local | `TIMESTAMP` |  |
| customer_verification_end_at_local | `TIMESTAMP` |  |
| cc_handling_start_at_local | `TIMESTAMP` |  |
| cc_handling_end_at_local | `TIMESTAMP` |  |
| dispatcher_start_at_local | `TIMESTAMP` |  |
| dispatcher_end_at_local | `TIMESTAMP` |  |
| order_placement_start_at_local | `TIMESTAMP` |  |
| order_placement_end_at_local | `TIMESTAMP` |  |
| idle_start_at_local | `TIMESTAMP` |  |
| idle_end_at_local | `TIMESTAMP` |  |
| online_payment_process_start_at_local | `TIMESTAMP` |  |
| online_payment_process_end_at_local | `TIMESTAMP` |  |
| order_completed_at_local | `TIMESTAMP` |  |
| cancelled_by_vendor_at_local | `TIMESTAMP` |  |
| cancelled_by_foodpanda_at_local | `TIMESTAMP` |  |
| cancelled_by_customer_at_local | `TIMESTAMP` |  |
| vendor_confirmation_start_at_local | `TIMESTAMP` |  |
| ordered_at_local | `TIMESTAMP` |  |
| status_updated_at_utc | `TIMESTAMP` |  |
| ordered_at_date_local | `DATE` |  |
| assigned_at_utc | `TIMESTAMP` |  |
| created_at_utc | `TIMESTAMP` |  |
| created_date_utc | `DATE` | Order created date in UTC and partitioned by this field |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |
| [delivery_provider](#deliveryprovider) | `RECORD` |  |
| [products](#products) | `ARRAY<RECORD>` |  |
| [calls](#calls) | `ARRAY<RECORD>` |  |
| [status_flows](#statusflows) | `ARRAY<RECORD>` |  |
| [payments](#payments) | `ARRAY<RECORD>` |  |

## delivery_provider

| Name | Type | Description |
| :--- | :--- | :---        |
| title | `STRING` |  |
| description | `STRING` |  |
| email | `STRING` |  |
| fax | `STRING` |  |
| mobile | `STRING` |  |
| phone | `STRING` |  |
| preferred_contact | `STRING` |  |
| third_party | `STRING` |  |
| is_active | `BOOLEAN` |  |
| is_type_own_delivery_foodpanda | `BOOLEAN` |  |
| is_type_own_delivery_third_party | `BOOLEAN` |  |
| is_type_vendor_delivery_third_party | `BOOLEAN` |  |
| has_express_delivery | `BOOLEAN` |  |
| type | `STRING` |  |

## products

| Name | Type | Description |
| :--- | :--- | :---        |
| title | `STRING` |  |
| description | `STRING` |  |
| variation_title | `STRING` |  |
| quantity | `INTEGER` |  |
| sold_out_option | `STRING` |  |
| special_instructions | `STRING` |  |
| is_half | `BOOLEAN` |  |
| vat_rate | `FLOAT` |  |
| container_price_local | `FLOAT` |  |
| discount_local | `FLOAT` |  |
| price_local | `FLOAT` |  |
| sponsorship_local | `FLOAT` |  |
| toppings_half_price_local | `FLOAT` |  |
| toppings_price_local | `FLOAT` |  |
| total_price_local | `FLOAT` |  |
| total_vat_amount_local | `NUMERIC` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |
| [toppings](#toppings) | `ARRAY<RECORD>` |  |

## toppings

| Name | Type | Description |
| :--- | :--- | :---        |
| title | `STRING` |  |
| half_position | `STRING` |  |
| price_local | `FLOAT` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |

## calls

| Name | Type | Description |
| :--- | :--- | :---        |
| created_by_user_id | `INTEGER` |  |
| updated_by_user_id | `INTEGER` |  |
| is_active | `BOOLEAN` |  |
| attempt_number | `INTEGER` |  |
| api_used | `STRING` |  |
| fail_reason | `STRING` |  |
| phone_number | `STRING` |  |
| executed_at_local | `TIMESTAMP` |  |
| scheduled_for_at_local | `TIMESTAMP` |  |
| created_at_local | `TIMESTAMP` |  |
| updated_at_local | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |

## status_flows

| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` |  |
| id | `INTEGER` |  |
| code | `INTEGER` |  |
| title | `STRING` |  |
| payment_status_type | `STRING` |  |
| is_earliest_status_flow | `BOOLEAN` |  |
| is_payment_status_pending | `BOOLEAN` |  |
| is_payment_status_fraud | `BOOLEAN` |  |
| is_payment_status_in_progress | `BOOLEAN` |  |
| is_payment_status_canceled_to_wallet | `BOOLEAN` |  |
| is_payment_status_refused | `BOOLEAN` |  |
| is_payment_status_refund | `BOOLEAN` |  |
| is_payment_status_cash_on_delivery | `BOOLEAN` |  |
| is_payment_status_payed | `BOOLEAN` |  |
| is_payment_status_error | `BOOLEAN` |  |
| is_payment_status_soft_paid | `BOOLEAN` |  |
| is_payment_status_canceled | `BOOLEAN` |  |
| is_payment_status_paid | `BOOLEAN` |  |
| is_cart_editable | `BOOLEAN` |  |
| is_deprecated | `BOOLEAN` |  |
| is_traffic_manager_declined | `BOOLEAN` |  |
| is_valid_order | `BOOLEAN` |  |
| is_test_order | `BOOLEAN` |  |
| is_final_order_state | `BOOLEAN` |  |
| is_failed_order | `BOOLEAN` |  |
| is_failed_order_vendor | `BOOLEAN` |  |
| is_failed_order_customer | `BOOLEAN` |  |
| is_failed_order_foodpanda | `BOOLEAN` |  |
| is_cancelled_by_foodpanda | `BOOLEAN` |  |
| is_cancelled_automatically | `BOOLEAN` |  |
| is_not_automated | `BOOLEAN` |  |
| is_replaced_order | `BOOLEAN` |  |
| is_incompleted_order | `BOOLEAN` |  |
| is_completed_order | `BOOLEAN` |  |
| is_gross_order | `BOOLEAN` |  |
| is_idle_start | `BOOLEAN` |  |
| is_online_payment_process_start | `BOOLEAN` |  |
| is_online_payment_process_end | `BOOLEAN` |  |
| is_customer_verification_start | `BOOLEAN` |  |
| is_customer_verification_end | `BOOLEAN` |  |
| is_credit_card_handling_start | `BOOLEAN` |  |
| is_credit_card_handling_end | `BOOLEAN` |  |
| is_dispatcher_start | `BOOLEAN` |  |
| is_dispatcher_end | `BOOLEAN` |  |
| is_order_placement_start | `BOOLEAN` |  |
| is_order_placement_end | `BOOLEAN` |  |
| created_at_local | `TIMESTAMP` |  |
| created_at_utc | `TIMESTAMP` |  |

## payments

| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` |  |
| id | `INTEGER` |  |
| amount_local | `FLOAT` |  |
| payment_method_type | `STRING` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |
