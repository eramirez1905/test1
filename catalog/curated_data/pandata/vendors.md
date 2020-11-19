# vendors

Table of vendors with each row representing one vendor.

Clustered by field `rdbms_id`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid is unique and represents a vendor |
| id | `INTEGER` |  |
| rdbms_id | `INTEGER` |  |
| area_uuid | `STRING` |  |
| area_id | `INTEGER` |  |
| calculation_configuration_template_uuid | `STRING` |  |
| calculation_configuration_template_id | `INTEGER` |  |
| chain_menu_group_uuid | `STRING` |  |
| chain_menu_group_id | `INTEGER` |  |
| city_uuid | `STRING` |  |
| city_id | `INTEGER` |  |
| delivery_provider_uuid | `STRING` |  |
| delivery_provider_id | `INTEGER` |  |
| language_uuid | `STRING` |  |
| language_id | `INTEGER` |  |
| option_value_vat_uuid | `STRING` |  |
| primary_cuisine_uuid | `STRING` |  |
| primary_cuisine_id | `INTEGER` |  |
| tag_uuid | `STRING` |  |
| tag_id | `INTEGER` |  |
| terms_uuid | `STRING` |  |
| terms_id | `INTEGER` |  |
| created_by_user_uuid | `STRING` |  |
| created_by_user_id | `INTEGER` |  |
| updated_by_user_uuid | `STRING` |  |
| updated_by_user_id | `INTEGER` |  |
| salesforce_id | `STRING` |  |
| code | `STRING` |  |
| address | `STRING` |  |
| address_other | `STRING` |  |
| automatic_calls_phone_number | `STRING` |  |
| customer_phone_number | `STRING` |  |
| branch | `STRING` |  |
| budget | `INTEGER` |  |
| description | `STRING` |  |
| filepath | `STRING` |  |
| foodpanda_phone | `STRING` |  |
| internal_comment | `STRING` |  |
| latitude | `NUMERIC` |  |
| longitude | `NUMERIC` |  |
| menu_style | `STRING` |  |
| order_email | `STRING` |  |
| order_fax | `STRING` |  |
| order_pos | `STRING` |  |
| order_sms_number | `STRING` |  |
| postcode | `STRING` |  |
| prefered_contact | `STRING` |  |
| rating | `INTEGER` |  |
| title | `STRING` |  |
| website | `STRING` |  |
| zoom | `INTEGER` |  |
| rider_pickup_instructions | `STRING` |  |
| vat_rate | `STRING` |  |
| customers_type | `STRING` |  |
| order_flow_type | `STRING` |  |
| pre_order_notification_type | `STRING` |  |
| rider_status_flow_type | `STRING` |  |
| automatic_calls_delay_in_minutes | `INTEGER` |  |
| minimum_delivery_time_in_minutes | `INTEGER` |  |
| pickup_time_in_minutes | `FLOAT` |  |
| pre_order_notification_time_in_minutes | `INTEGER` |  |
| preorder_offset_time_in_minutes | `INTEGER` |  |
| is_automation | `BOOLEAN` |  |
| is_customer_invitations_accepted | `BOOLEAN` |  |
| is_delivery_accepted | `BOOLEAN` |  |
| is_discount_accepted | `BOOLEAN` |  |
| is_pickup_accepted | `BOOLEAN` |  |
| is_voucher_accepted | `BOOLEAN` |  |
| is_commission_payment_enabled | `BOOLEAN` |  |
| is_special_instructions_accepted | `BOOLEAN` |  |
| is_active | `BOOLEAN` |  |
| is_deleted | `BOOLEAN` |  |
| is_crosslisted | `BOOLEAN` |  |
| is_automatic_calls_enabled | `BOOLEAN` |  |
| is_preorder_allowed | `BOOLEAN` |  |
| is_customers_type_all | `BOOLEAN` |  |
| is_customers_type_regular | `BOOLEAN` |  |
| is_customers_type_corporate | `BOOLEAN` |  |
| is_order_flow_food | `BOOLEAN` |  |
| is_order_flow_grocery | `BOOLEAN` |  |
| is_mobile_app_disabled | `BOOLEAN` |  |
| is_frontpage_displayed | `BOOLEAN` |  |
| is_display_only | `BOOLEAN` |  |
| is_split_payment_enabled | `BOOLEAN` |  |
| is_menu_inherited | `BOOLEAN` |  |
| is_schedule_inherited | `BOOLEAN` |  |
| is_checkout_comment_enabled | `BOOLEAN` |  |
| is_closing_after_declined_order_disabled | `BOOLEAN` |  |
| is_new | `BOOLEAN` |  |
| is_private | `BOOLEAN` |  |
| is_test | `BOOLEAN` |  |
| is_replacement_dish_enabled | `BOOLEAN` |  |
| is_offline_calls_disabled | `BOOLEAN` |  |
| is_offline_calls_status_closed | `BOOLEAN` |  |
| is_popular_products_enabled | `BOOLEAN` |  |
| is_multi_branch | `BOOLEAN` |  |
| is_invoice_sent | `BOOLEAN` |  |
| is_address_verification_required | `BOOLEAN` |  |
| is_rider_pickup_preassignment_enabled | `BOOLEAN` |  |
| is_premium_listing | `BOOLEAN` |  |
| is_pickup_time_set_by_vendor | `BOOLEAN` |  |
| is_vat_included | `BOOLEAN` |  |
| is_vat_visible | `BOOLEAN` |  |
| is_best_in_city | `BOOLEAN` |  |
| has_free_delivery | `BOOLEAN` |  |
| has_delivery_charge | `BOOLEAN` |  |
| has_service_charge | `BOOLEAN` |  |
| has_temporary_adjustment_fees | `BOOLEAN` |  |
| has_weekly_adjustment_fees | `BOOLEAN` |  |
| basket_adjustment_fees_local | `INTEGER` |  |
| container_price_local | `NUMERIC` |  |
| commission_local | `FLOAT` |  |
| pickup_commission_local | `FLOAT` |  |
| maximum_delivery_fee_local | `FLOAT` |  |
| maximum_express_order_amount_local | `FLOAT` |  |
| minimum_delivery_fee_local | `FLOAT` |  |
| minimum_delivery_value_local | `FLOAT` |  |
| service_fee_local | `FLOAT` |  |
| new_until_local | `TIMESTAMP` |  |
| terms_valid_until_date | `DATE` |  |
| created_at_local | `TIMESTAMP` |  |
| updated_at_local | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |
| [chain](#chain) | `RECORD` |  |
| [menu_categories](#menucategories) | `ARRAY<RECORD>` |  |
| [flows](#flows) | `ARRAY<RECORD>` |  |
| [discounts](#discounts) | `ARRAY<RECORD>` |  |
| [payment_types](#paymenttypes) | `ARRAY<RECORD>` |  |
| [menus](#menus) | `ARRAY<RECORD>` |  |
| [configurations](#configurations) | `ARRAY<RECORD>` |  |
| [food_characteristics](#foodcharacteristics) | `ARRAY<RECORD>` |  |
| [schedules](#schedules) | `ARRAY<RECORD>` |  |
| [special_schedules](#specialschedules) | `ARRAY<RECORD>` |  |
| [cuisines](#cuisines) | `ARRAY<RECORD>` |  |

## chain

| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` |  |
| id | `INTEGER` |  |
| title | `STRING` |  |
| code | `STRING` |  |
| is_main_vendor | `BOOLEAN` |  |
| is_active | `BOOLEAN` |  |
| is_deleted | `BOOLEAN` |  |
| is_accepting_global_vouchers | `BOOLEAN` |  |
| is_always_grouping_by_chain_on_listing_page | `BOOLEAN` |  |
| is_chain_stacking_required | `BOOLEAN` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |
| [menu_groups](#menugroups) | `ARRAY<RECORD>` |  |

## menu_groups

| Name | Type | Description |
| :--- | :--- | :---        |
| main_vendor_uuid | `STRING` |  |
| main_vendor_id | `INTEGER` |  |
| title | `STRING` |  |
| description | `STRING` |  |
| is_deleted | `BOOLEAN` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |

## menu_categories

| Name | Type | Description |
| :--- | :--- | :---        |
| title | `STRING` |  |
| description | `STRING` |  |
| position | `INTEGER` |  |
| is_deleted | `BOOLEAN` |  |
| is_shown_without_products | `BOOLEAN` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |

## flows

| Name | Type | Description |
| :--- | :--- | :---        |
| user_uuid | `STRING` |  |
| user_id | `INTEGER` |  |
| username | `STRING` |  |
| type | `STRING` |  |
| value | `STRING` |  |
| start_at_local | `TIMESTAMP` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |

## discounts

| Name | Type | Description |
| :--- | :--- | :---        |
| discount_type | `STRING` |  |
| title | `STRING` |  |
| description | `STRING` |  |
| banner_title | `STRING` |  |
| condition_type | `STRING` |  |
| is_condition_multiple_vendors | `BOOLEAN` |  |
| discount_text | `STRING` |  |
| amount_local | `NUMERIC` |  |
| minimum_order_value_local | `FLOAT` |  |
| foodpanda_ratio | `FLOAT` |  |
| is_active | `BOOLEAN` |  |
| is_deleted | `BOOLEAN` |  |
| start_date_local | `DATE` |  |
| end_date_local | `DATE` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |

## payment_types

| Name | Type | Description |
| :--- | :--- | :---        |
| title | `STRING` |  |
| description | `STRING` |  |
| position | `INTEGER` |  |
| checkout_text | `STRING` |  |
| brand_code_type | `STRING` |  |
| code_type | `STRING` |  |
| code_sub_type | `STRING` |  |
| payment_method_type | `STRING` |  |
| is_delivery_enabled | `BOOLEAN` |  |
| is_hosted | `BOOLEAN` |  |
| is_pickup_enabled | `BOOLEAN` |  |
| is_tokenisation_checked | `BOOLEAN` |  |
| is_tokenisation_enabled | `BOOLEAN` |  |
| has_need_for_change | `BOOLEAN` |  |
| is_active | `BOOLEAN` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |

## menus

| Name | Type | Description |
| :--- | :--- | :---        |
| available_weekdays | `INTEGER` |  |
| description | `STRING` |  |
| position | `INTEGER` |  |
| title | `STRING` |  |
| menu_type | `STRING` |  |
| is_active | `BOOLEAN` |  |
| is_deleted | `BOOLEAN` |  |
| start_time_local | `TIME` |  |
| stop_time_local | `TIME` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |

## configurations

| Name | Type | Description |
| :--- | :--- | :---        |
| rider_payment_handling_type | `STRING` |  |
| rider_payout_type | `STRING` |  |
| vendor_prepayment_type | `STRING` |  |
| is_latest | `BOOLEAN` |  |
| is_minimum_order_value_difference_charged | `BOOLEAN` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |

## food_characteristics

| Name | Type | Description |
| :--- | :--- | :---        |
| title | `STRING` |  |
| is_active | `BOOLEAN` |  |
| is_halal | `BOOLEAN` |  |
| is_vegetarian | `BOOLEAN` |  |
| has_mobile_filter | `BOOLEAN` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |

## schedules

| Name | Type | Description |
| :--- | :--- | :---        |
| start_time_local | `TIME` |  |
| stop_time_local | `TIME` |  |
| day_number | `INTEGER` |  |
| day_in_words | `STRING` |  |
| type | `STRING` |  |
| is_all_day | `BOOLEAN` |  |
| is_type_delivering | `BOOLEAN` |  |
| is_type_opened | `BOOLEAN` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |

## special_schedules

| Name | Type | Description |
| :--- | :--- | :---        |
| start_time_local | `TIME` |  |
| stop_time_local | `TIME` |  |
| type | `STRING` |  |
| is_all_day | `BOOLEAN` |  |
| is_type_delivering | `BOOLEAN` |  |
| is_type_opened | `BOOLEAN` |  |
| is_type_closed | `BOOLEAN` |  |
| is_type_busy | `BOOLEAN` |  |
| is_type_unavailable | `BOOLEAN` |  |
| start_date_local | `DATE` |  |
| end_date_local | `DATE` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |

## cuisines

| Name | Type | Description |
| :--- | :--- | :---        |
| code | `STRING` |  |
| title | `STRING` |  |
| global_title | `STRING` |  |
| is_main_cuisine | `BOOLEAN` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
