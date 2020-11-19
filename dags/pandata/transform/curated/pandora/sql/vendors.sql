WITH vendors_agg_vendor_flows AS (
  SELECT
    vendor_uuid,
    ARRAY_AGG(
      STRUCT(
        user_uuid,
        user_id,
        username,
        type,
        value,
        start_at_local,
        created_at_utc,
        updated_at_utc,
        dwh_last_modified_at_utc
      )
    ) AS flows
  FROM `{project_id}.pandata_intermediate.pd_vendor_flows`
  GROUP BY vendor_uuid
),

discounts_agg_discount_schedules AS (
  SELECT
    discount_uuid,
    ARRAY_AGG(
      STRUCT(
        start_time,
        end_time,
        weekday
      )
    ) AS schedules
  FROM `{project_id}.pandata_intermediate.pd_discount_schedules`
  GROUP BY discount_uuid
),

vendor_discounts AS (
  -- mutliple vendors discounts
  SELECT
    vendor_discounts.vendor_uuid,
    discounts.discount_type,
    discounts.title,
    discounts.description,
    discounts.banner_title,
    discounts.condition_type,
    discounts.is_condition_multiple_vendors,
    discounts.discount_text,
    discounts.amount_local,
    discounts.minimum_order_value_local,
    discounts.foodpanda_ratio,
    discounts.is_active,
    vendor_discounts.is_deleted,
    discounts.start_date_local,
    discounts.start_time_local,
    discounts.end_date_local,
    discounts.end_time_local,
    vendor_discounts.created_at_utc,
    vendor_discounts.updated_at_utc,
    vendor_discounts.dwh_last_modified_at_utc,
    discounts_agg_discount_schedules.schedules,
  FROM `{project_id}.pandata_intermediate.pd_vendor_discounts` AS vendor_discounts
  INNER JOIN `{project_id}.pandata_intermediate.pd_discounts` AS discounts
          ON discounts.uuid = vendor_discounts.discount_uuid
  LEFT JOIN discounts_agg_discount_schedules
         ON discounts.uuid = discounts_agg_discount_schedules.discount_uuid
  WHERE discounts.is_condition_multiple_vendors

  UNION ALL

  -- not multiple vendor discounts
  SELECT
    discounts.vendor_uuid,
    discounts.discount_type,
    discounts.title,
    discounts.description,
    discounts.banner_title,
    discounts.condition_type,
    discounts.is_condition_multiple_vendors,
    discounts.discount_text,
    discounts.amount_local,
    discounts.minimum_order_value_local,
    discounts.foodpanda_ratio,
    discounts.is_active,
    discounts.is_deleted,
    discounts.start_date_local,
    discounts.start_time_local,
    discounts.end_date_local,
    discounts.end_time_local,
    discounts.created_at_utc,
    discounts.updated_at_utc,
    discounts.dwh_last_modified_at_utc,
    discounts_agg_discount_schedules.schedules,
  FROM `{project_id}.pandata_intermediate.pd_discounts` AS discounts
  LEFT JOIN discounts_agg_discount_schedules
         ON discounts.uuid = discounts_agg_discount_schedules.discount_uuid
  WHERE discounts.vendor_uuid IS NOT NULL
    AND NOT discounts.is_condition_multiple_vendors
),

vendors_agg_discounts AS (
  SELECT
    vendor_uuid,
    ARRAY_AGG(
      STRUCT(
        discount_type,
        title,
        description,
        banner_title,
        condition_type,
        is_condition_multiple_vendors,
        discount_text,
        amount_local,
        minimum_order_value_local,
        foodpanda_ratio,
        is_active,
        is_deleted,
        start_date_local,
        end_date_local,
        created_at_utc,
        updated_at_utc,
        dwh_last_modified_at_utc
      )
    ) AS discounts,
  FROM vendor_discounts
  GROUP BY vendor_uuid
),

vendors_agg_payment_types AS (
  SELECT
    vendors_payment_types.vendor_uuid,
    ARRAY_AGG(
      STRUCT(
        payment_types.title,
        payment_types.description,
        payment_types.position,
        payment_types.checkout_text,
        payment_types.brand_code_type,
        payment_types.code_type,
        payment_types.code_sub_type,
        payment_types.payment_method_type,
        payment_types.is_delivery_enabled,
        payment_types.is_hosted,
        payment_types.is_pickup_enabled,
        payment_types.is_tokenisation_checked,
        payment_types.is_tokenisation_enabled,
        payment_types.has_need_for_change,
        vendors_payment_types.is_active AND payment_types.is_active AS is_active,
        vendors_payment_types.created_at_utc,
        vendors_payment_types.updated_at_utc,
        vendors_payment_types.dwh_last_modified_at_utc
      )
    ) AS payment_types
  FROM `{project_id}.pandata_intermediate.pd_vendors_payment_types` AS vendors_payment_types
  LEFT JOIN `{project_id}.pandata_intermediate.pd_payment_types` AS payment_types
         ON vendors_payment_types.payment_type_uuid = payment_types.uuid
  GROUP BY vendors_payment_types.vendor_uuid
),

vendors_agg_menu_categories AS (
  SELECT
    vendor_uuid,
    ARRAY_AGG(
      STRUCT(
        title,
        description,
        position,
        is_deleted,
        is_shown_without_products,
        created_at_utc,
        updated_at_utc,
        dwh_last_modified_at_utc
      )
    ) AS menu_categories,
  FROM `{project_id}.pandata_intermediate.pd_menu_categories`
  GROUP BY vendor_uuid
),

vendors_agg_menus AS (
  SELECT
    vendor_uuid,
    ARRAY_AGG(
      STRUCT(
        available_weekdays,
        description,
        position,
        title,
        menu_type,
        is_active,
        is_deleted,
        start_time_local,
        stop_time_local,
        created_at_utc,
        updated_at_utc,
        dwh_last_modified_at_utc
      )
    ) AS menus
  FROM `{project_id}.pandata_intermediate.pd_menus`
  GROUP BY vendor_uuid
),

vendors_agg_configurations AS (
  SELECT
    vendor_uuid,
    ARRAY_AGG(
      STRUCT(
        rider_payment_handling_type,
        rider_payout_type,
        vendor_prepayment_type,
        is_latest,
        is_minimum_order_value_difference_charged,
        created_at_utc,
        updated_at_utc,
        dwh_last_modified_at_utc
      )
    ) AS configurations
  FROM `{project_id}.pandata_intermediate.pd_vendor_configuration`
  GROUP BY vendor_uuid
),

vendors_agg_food_characteristics AS (
  SELECT
    vendors_food_characteristics.vendor_uuid,
    ARRAY_AGG(
      STRUCT(
        food_characteristics.title,
        food_characteristics.is_active,
        food_characteristics.is_halal,
        food_characteristics.is_vegetarian,
        food_characteristics.has_mobile_filter,
        food_characteristics.dwh_last_modified_at_utc
      )
    ) AS food_characteristics
  FROM `{project_id}.pandata_intermediate.pd_vendors_food_characteristics` AS vendors_food_characteristics
  LEFT JOIN `{project_id}.pandata_intermediate.pd_food_characteristics` AS food_characteristics
         ON vendors_food_characteristics.food_characteristic_uuid = food_characteristics.uuid
  GROUP BY vendors_food_characteristics.vendor_uuid
),

vendors_agg_schedules AS (
  SELECT
    vendor_uuid,
    ARRAY_AGG(
      STRUCT(
        start_time_local,
        stop_time_local,
        day_number,
        day_in_words,
        type,
        is_all_day,
        is_type_delivering,
        is_type_opened,
        created_at_utc,
        updated_at_utc,
        dwh_last_modified_at_utc
      )
    ) AS schedules,
  FROM `{project_id}.pandata_intermediate.pd_schedules`
  GROUP BY vendor_uuid
),

vendors_agg_special_schedules AS (
  SELECT
    vendor_uuid,
    ARRAY_AGG(
      STRUCT(
        start_time_local,
        stop_time_local,
        type,
        is_all_day,
        is_type_delivering,
        is_type_opened,
        is_type_closed,
        is_type_busy,
        is_type_unavailable,
        start_date_local,
        end_date_local,
        created_at_utc,
        updated_at_utc,
        dwh_last_modified_at_utc
      )
    ) AS special_schedules,
  FROM `{project_id}.pandata_intermediate.pd_special_schedules`
  GROUP BY vendor_uuid
),

vendor_dispatcher_flags AS (
  SELECT DISTINCT
    rdbms_id,
    k AS vendor_code
  FROM `{project_id}.pandata_intermediate.pd_configuration`
  WHERE is_group_code_dispatcher
),

vendors_agg_cuisines AS (
  SELECT
    vendors_cuisines.vendor_uuid,
    ARRAY_AGG(
      STRUCT(
        cuisines.code,
        cuisines.title,
        cuisines.global_title,
        vendors_cuisines.is_main_cuisine,
        vendors_cuisines.created_at_utc,
        vendors_cuisines.updated_at_utc
      )
    ) AS cuisines
  FROM `{project_id}.pandata_intermediate.pd_vendors_cuisines` AS vendors_cuisines
  LEFT JOIN `{project_id}.pandata_intermediate.pd_cuisines` AS cuisines
         ON vendors_cuisines.cuisine_uuid = cuisines.uuid
  LEFT JOIN `{project_id}.pandata_intermediate.pd_global_cuisines` AS global_cuisines
         ON cuisines.global_cuisine_uuid = global_cuisines.uuid
  GROUP BY vendors_cuisines.vendor_uuid
),

chains_agg_menu_groups AS (
  SELECT
    chain_uuid,
    ARRAY_AGG(
      STRUCT(
        main_vendor_uuid,
        main_vendor_id,
        title,
        description,
        is_deleted,
        dwh_last_modified_at_utc
      )
    ) AS menu_groups
  FROM `{project_id}.pandata_intermediate.pd_chain_menu_groups`
  GROUP BY chain_uuid
)

SELECT
  vendors.uuid,
  vendors.id,
  vendors.rdbms_id,
  vendors.area_uuid,
  vendors.area_id,
  vendors.calculation_configuration_template_uuid,
  vendors.calculation_configuration_template_id,
  vendors.chain_menu_group_uuid,
  vendors.chain_menu_group_id,
  vendors.city_uuid,
  vendors.city_id,
  vendors.delivery_provider_uuid,
  vendors.delivery_provider_id,
  vendors.language_uuid,
  vendors.language_id,
  vendors.option_value_vat_uuid,
  vendors.primary_cuisine_uuid,
  vendors.primary_cuisine_id,
  vendors.tag_uuid,
  vendors.tag_id,
  vendors.terms_uuid,
  vendors.terms_id,
  vendors.created_by_user_uuid,
  vendors.created_by_user_id,
  vendors.updated_by_user_uuid,
  vendors.updated_by_user_id,
  vendor_additional_info.salesforce_id,

  vendors.code,
  vendors.address,
  vendors.address_other,
  vendors.automatic_calls_phone_number,
  vendors.customer_phone_number,
  vendors.branch,
  vendors.budget,
  vendors.description,
  vendors.filepath,
  vendors.foodpanda_phone,
  vendors.internal_comment,
  vendors.latitude,
  vendors.longitude,
  vendors.menu_style,
  vendors.order_email,
  vendors.order_fax,
  vendors.order_pos,
  vendors.order_sms_number,
  vendors.postcode,
  vendors.prefered_contact,
  vendors.rating,
  vendors.title,
  vendors.website,
  vendors.zoom,
  vendor_additional_info.rider_pickup_instructions,
  option_values.vat_rate,

  -- types
  vendors.customers_type,
  vendors.order_flow_type,
  vendors.pre_order_notification_type,
  vendors.rider_status_flow_type,

  -- time
  vendors.automatic_calls_delay_in_minutes,
  vendors.minimum_delivery_time_in_minutes,
  vendors.pickup_time_in_minutes,
  vendors.pre_order_notification_time_in_minutes,
  vendor_additional_info.preorder_offset_time_in_minutes,

  -- booleans
  (
    vendors.prefered_contact LIKE '%dispatcher%' AND vendor_dispatcher_flags IS NOT NULL)  -- printer and pos
    OR vendors.prefered_contact LIKE '%order_vendorbackend%'  -- vendor app
    OR vendors.prefered_contact LIKE '%desktopapp%' -- order notifier
  AS is_automation,
  vendors.is_customer_invitations_accepted,
  vendors.is_delivery_accepted,
  vendors.is_discount_accepted,
  vendors.is_pickup_accepted,
  vendors.is_voucher_accepted,
  vendors.is_commission_payment_enabled,
  vendors.is_special_instructions_accepted,
  vendors.is_active,
  vendors.is_deleted,
  vendors.is_crosslisted,
  vendors.is_automatic_calls_enabled,
  vendors.is_preorder_allowed,
  vendors.is_customers_type_all,
  vendors.is_customers_type_regular,
  vendors.is_customers_type_corporate,
  vendors.is_order_flow_food,
  vendors.is_order_flow_grocery,
  vendors.is_mobile_app_disabled,
  vendors.is_frontpage_displayed,
  vendors.is_display_only,
  vendors.is_split_payment_enabled,
  vendors.is_menu_inherited,
  vendors.is_schedule_inherited,
  vendors.is_checkout_comment_enabled,
  vendors.is_closing_after_declined_order_disabled,
  vendors.is_new,
  vendors.is_private,
  vendors.is_test,
  vendors.is_replacement_dish_enabled,
  vendors.is_offline_calls_disabled,
  vendors.is_offline_calls_status_closed,
  vendors.is_popular_products_enabled,
  vendors.is_multi_branch,
  vendors.is_invoice_sent,
  vendors.is_address_verification_required,
  vendors.is_rider_pickup_preassignment_enabled,
  vendors.is_premium_listing,
  vendors.is_pickup_time_set_by_vendor,
  vendors.is_vat_included,
  vendors.is_vat_visible,
  vendor_additional_info.is_best_in_city,
  vendors.has_free_delivery,
  vendors.has_delivery_charge,
  vendors.has_service_charge,
  vendors.has_temporary_adjustment_fees,
  vendors.has_weekly_adjustment_fees,

  -- currency
  vendors.basket_adjustment_fees_local,
  vendors.container_price_local,
  vendors.commission_local,
  vendors.pickup_commission_local,
  vendors.maximum_delivery_fee_local,
  vendors.maximum_express_order_amount_local,
  vendors.minimum_delivery_fee_local,
  vendors.minimum_delivery_value_local,
  vendors.service_fee_local,

  -- timestamps
  vendors.new_until_local,
  vendors.terms_valid_until_date,
  vendors.created_at_local,
  vendors.updated_at_local,
  vendors.dwh_last_modified_at_utc,

  -- records
  STRUCT(
    chains.uuid,
    chains.id,
    chains.title,
    chains.code,
    vendors.uuid = chains.main_vendor_uuid AS is_main_vendor,
    chains.is_active,
    chains.is_deleted,
    chains.is_accepting_global_vouchers,
    chains.is_always_grouping_by_chain_on_listing_page,
    chains.is_chain_stacking_required,
    chains.created_at_utc,
    chains.updated_at_utc,
    chains.dwh_last_modified_at_utc,
    chains_agg_menu_groups.menu_groups
  ) AS chain,

  -- repeated records
  vendors_agg_menu_categories.menu_categories,
  vendors_agg_vendor_flows.flows,
  vendors_agg_discounts.discounts,
  vendors_agg_payment_types.payment_types,
  vendors_agg_menus.menus,
  vendors_agg_configurations.configurations,
  vendors_agg_food_characteristics.food_characteristics,
  vendors_agg_schedules.schedules,
  vendors_agg_special_schedules.special_schedules,
  vendors_agg_cuisines.cuisines,
FROM `{project_id}.pandata_intermediate.pd_vendors` AS vendors
LEFT JOIN `{project_id}.pandata_intermediate.pd_vendors_additional_info` AS vendor_additional_info
       ON vendors.uuid = vendor_additional_info.vendor_uuid
LEFT JOIN `{project_id}.pandata_intermediate.pd_vendor_chains` AS chains
       ON vendors.chain_uuid = chains.uuid
LEFT JOIN `{project_id}.pandata_intermediate.pd_option_values` AS option_values
       ON vendors.option_value_vat_uuid = option_values.uuid
LEFT JOIN chains_agg_menu_groups
       ON chains.uuid = chains_agg_menu_groups.chain_uuid
LEFT JOIN vendors_agg_vendor_flows
       ON vendors.uuid = vendors_agg_vendor_flows.vendor_uuid
LEFT JOIN vendors_agg_discounts
       ON vendors.uuid = vendors_agg_discounts.vendor_uuid
LEFT JOIN vendors_agg_payment_types
       ON vendors.uuid = vendors_agg_payment_types.vendor_uuid
LEFT JOIN vendors_agg_menu_categories
       ON vendors.uuid = vendors_agg_menu_categories.vendor_uuid
LEFT JOIN vendors_agg_menus
       ON vendors.uuid = vendors_agg_menus.vendor_uuid
LEFT JOIN vendors_agg_configurations
       ON vendors.uuid = vendors_agg_configurations.vendor_uuid
LEFT JOIN vendors_agg_food_characteristics
       ON vendors.uuid = vendors_agg_food_characteristics.vendor_uuid
LEFT JOIN vendors_agg_schedules
       ON vendors.uuid = vendors_agg_schedules.vendor_uuid
LEFT JOIN vendors_agg_special_schedules
       ON vendors.uuid = vendors_agg_special_schedules.vendor_uuid
LEFT JOIN vendors_agg_cuisines
       ON vendors.uuid = vendors_agg_cuisines.vendor_uuid
LEFT JOIN vendor_dispatcher_flags
       ON vendors.rdbms_id = vendor_dispatcher_flags.rdbms_id
      AND vendors.code = vendor_dispatcher_flags.vendor_code
