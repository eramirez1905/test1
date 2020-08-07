DECLARE FILTER_DATE DATE DEFAULT date('@dateStr');

delete from  `peya-data-origins-stg.origin_data_refined.partner_origin`
where partner_id in (select id
                        from `peya-data-origins-stg.origins_data_stg.sqs_partner`
                        where yyyymmdd = FILTER_DATE
                       );

insert into `peya-data-origins-stg.origin_data_refined.partner_origin`
    (
        accepts_pre_order,
        accepts_vouchers,
        city_id,
        area_id,
        address_door_number,
        address_id,
        address_phone,
        address_street,
        affected_by_porygon_events,
        affected_by_porygon_optimizations,
        automatic_phone,
        automatic_phone_enabled,
        billing_info_id,
        branch_id,
        branch_name,
        business_name,
        business_type,
        capacity_check,
        country_id,
        country_url_site,
        created_date_id,
        created_date,
        delivery_time_description,
        delivery_time_id,
        delivery_time_max_minutes,
        delivery_time_min_minutes,
        delivery_time,
        delivery_time_order,
        delivery_type,
        description,
        disabled_reason,
        header_image,
        home_vip,
        partner_id,
        identity_card_behaviour,
        integration_code,
        integration_name,
        is_darkstore,
        is_important_account,
        is_vip,
        last_updated,
        link,
        logo,
        mandatory_address_confirmation,
        mandatory_identity_card,
        mandatory_payment_amount,
        max_shipping_amount,
        has_banner,
        url_site,
        accepts_and_supports_vouchers,
        declines_but_supports_vouchers,
        menu_id,
        menu_name,
        message_id,
        message_timestamp,
        backend_id,
        min_delivery_amount,
        partner_name,
        partner_noindex,
        partner_noindex_google_places,
        orders_reception_system_id,
        has_pos,
        reception_system_name,
        orders_secondary_reception_system_id,
        orders_secondary_reception_system_ispos,
        orders_secondary_reception_system_name,
        parallel_reception_system,
        has_online_payment,
        private_phone,
        public_phone,
        reception_system_enabled,
        registered_date,
        registered_date_id,
        is_new_registered,
        centralized_reception_partner_id,
        partner_brand_name,
        partner_trust_score_id,
        salesforce_id,
        shipping_amount,
        has_shipping_amount,
        shipping_amount_is_percentage,
        stamps_needed,
        has_stamps,
        stamps_state,
        partner_state,
        is_pending,
        is_offline,
        is_online,
        is_premium,
        is_express,
        is_talent,
        partner_type,
        is_logistic,
        business_type_id,
        delivery_type_id,
        stamps_state_id,
        partner_state_id,
        partner_type_id,
        main_cousine_category_id,
        main_cousine_category_name,
        concept_id,
        kitchen_id,
        business_category_id,
        requires_proof_of_delivery
    )
WITH partner_food_categories as (
   select
                  t.id                   as partner_id
                , fc.foodCategory.id     as food_category_id
                , fc.foodCategory.name   as food_category_name
                , ROW_NUMBER() over(partition by t.id
                          order by fc.sortingIndex asc
                                , fc.percentage) as rank_food_category
                , fc.sortingIndex as sorting_index
                , fc.percentage
            from `peya-data-origins-stg.origins_data_stg.sqs_partner` t
                  cross join unnest(foodCategories) fc
            where fc.enabled
                and fc.state in ('AUTO_ON', 'MANUAL_ON', 'BI_ON')
                and yyyymmdd = FILTER_DATE
 )
SELECT
    acceptsPreOrder 	     as accepts_pre_order,
    acceptsVouchers 		 as accepts_vouchers,
    address.area.city.id as city_id,
    address.area.id 		 as area_id,
    address.doorNumber 	     as address_door_number,
    address.id 				 as address_id,
    address.phone 			 as address_phone,
    address.street 			 as address_street,
    affectedByPorygonEvents         as affected_by_porygon_events,
    affectedByPorygonOptimizations  as affected_by_porygon_optimizations,
    automaticPhone 			    as automatic_phone,
    automaticPhoneEnabled 	as automatic_phone_enabled,
    billingInfo.id 			    as billing_info_id,
    branchParent.id 		    as branch_id,
    branchParent.name 	    as branch_name,
    businessType 			      as business_name,
    businessType 			      as business_type,
    capacityCheck 			    as capacity_check,
    country.id 				      as country_id,
    country.url 			      as country_url_site,
    CAST(FORMAT_DATE('%Y%m%d', cast(dateCreated as DATE)) AS INT64) as created_date_id,
    dateCreated 			as created_date,
    deliveryTime.description as delivery_time_description,
    deliveryTime.id 		    as delivery_time_id,
    deliveryTime.maxMinutes as delivery_time_max_minutes,
    deliveryTime.minMinutes as delivery_time_min_minutes,
    deliveryTime.name 		  as delivery_time,
    deliveryTime.order 		  as delivery_time_order,
    deliveryType 			      as delivery_type,
    description 			      as description,
    disabledReason 			        as disabled_reason,
    headerImage 			        as header_image,
    homeVip 				        as home_vip,
    p.id 						    as partner_id,
    identityCardBehaviour 	as identity_card_behaviour,
    integrationCode 		    as integration_code,
    integrationName 		    as integration_name,
    coalesce(isDarkstore, False) as is_darkstore,
    isImportantAccount 		  as is_important_account,
    isVip 					        as is_vip,
    lastUpdated 			      as last_updated,
    link 					          as link,
    logo 					          as logo,
    mandatoryAddressConfirmation as mandatory_address_confirmation,
    mandatoryIdentityCard 	as mandatory_identity_card,
    mandatoryPaymentAmount 	as mandatory_payment_amount,
    maxShippingAmount 		  as max_shipping_amount,
    case
        when (menu.bannerUrl is null
              or length(menu.bannerUrl) < 1) then false
        else true
    end                     as has_banner,
    country.url || 'restaurantes/' || address.area.city.slug || '/' || p.link || '-menu' as url_site,
    --  as has_custom_photo_menu
    case
        when (p.acceptsVouchers
                and ordersReceptionSystem.supportsVouchers
                and (ordersSecondaryReceptionSystem.id is null
                        or ordersSecondaryReceptionSystem.supportsVouchers)
              ) then true
        else false
    end	                    as accepts_and_supports_vouchers,
    case
        when (not p.acceptsVouchers
                and ordersReceptionSystem.supportsVouchers
                and (ordersSecondaryReceptionSystem.id is null
                        or ordersSecondaryReceptionSystem.supportsVouchers
                )
            ) then true
        else false
    end                             as declines_but_supports_vouchers,
    menu.id 				        as menu_id,
    menu.name 				        as menu_name,
    messageId 				        as message_id,
    messageTimestamp 		        as message_timestamp,
    migrationId 			        as backend_id,
    minDeliveryAmount 		        as min_delivery_amount,
    p.name 					        as partner_name,
    noIndex 				        as partner_noindex,
    noIndexGooglePlaces 	        as partner_noindex_google_places,
    ordersReceptionSystem.id 	    as orders_reception_system_id,
    ordersReceptionSystem.isPos     as has_pos,
    ordersReceptionSystem.name 	    as reception_system_name,
    ordersSecondaryReceptionSystem.id 			  as orders_secondary_reception_system_id,
    ordersSecondaryReceptionSystem.isPos 		  as orders_secondary_reception_system_ispos,
    ordersSecondaryReceptionSystem.name 		  as orders_secondary_reception_system_name,
    coalesce(parallelReceptionSystem, False) 	as parallel_reception_system,
    case
      when true IN (select online from UNNEST(paymentMethods)) then True
      else False
	  end  										                  as has_online_payment,
    -- col('paymentMethods[].online = true as has_online_payment,
    privatePhone 		as private_phone,
    publicPhone 		as public_phone,
    coalesce(receptionSystemEnabled, False)     as reception_system_enabled,
    registeredDate 								            as registered_date,
    CAST(FORMAT_DATE('%Y%m%d', CAST(registeredDate AS DATE)) as int64) as registered_date_id,
    --col('registeredDate as is_new_registered,
    case
        when CAST(registeredDate AS DATE) = CAST(date_add(CURRENT_DATE(), INTERVAL -1 DAY) AS DATE) then True
        else False
	end 										                                          as is_new_registered,
    --col('restaurantBrand.id is not null as is_chain,
    /*case
		  when(restaurantBrand.id is not null then True
		  else False
	  end  										          as is_chain,*/
    restaurantBrand.id 							as centralized_reception_partner_id,
    restaurantBrand.name 						as partner_brand_name,
    restaurantTrustScore.id 				    as partner_trust_score_id,
    salesforceId 								as salesforce_id,
    shippingAmount 								as shipping_amount,
    case
      when shippingAmount > 0 then True
      else False
	end  										as has_shipping_amount,
    shippingAmountIsPercentage 	    as shipping_amount_is_percentage,
    stampsNeeded 								as stamps_needed,
    case
		when(stampsNeeded > 0
				and stampsState = 'ACTIVE') then True
		else False
	end  										  as has_stamps,
    stampsState 							      as stamps_state,
    -- 1 ON_LINE | 2 RETENTION |3 PENDING | 4	UPDATING | 5	DELETED | 6	CLOSED
    state 										  as partner_state,
    case
		when state = 'PENDING' then True
		else False
	end  										  as is_pending,
    --col('state not in ("1", "4") && type <> "3" as is_offline,
    case
		when p.state != 'ON_LINE'
				  and p.state != 'UPDATING'
				  and p.type != 'DELIVERY' then true
        else False
	end  										  as is_offline,
    --col('state in ("1", "4") && type = 3 as is_online,
    case
		when(p.state = 'ON_LINE' or state = 'UPDATING')
				and p.type = 'DELIVERY' then True
		else False
	end  										 as is_online,
    case
		when '##DELIVERY_PREMIUM' IN UNNEST(tags) then True
		else False
	end 										 as is_premium,
    case
		when '##delivery-express' IN UNNEST(tags) then True
		else False
	end  										 as is_express,
    coalesce(talent, false) 			         as is_talent,
    p.type 										 as partner_type,
    coalesce(useLiveOrderTracking, false) 		 as is_logistic,
    coalesce(pbt.id, 0)                          as business_type_id,
    coalesce(pdt.id, 0)                          as delivery_type_id,
    coalesce(pss.id, 0)                          as stamps_state_id,
    coalesce(ps.id, 0)                           as partner_state_id,
    coalesce(pt.id, 0)                           as partner_type_id,
    --bi.commission,
		--bi.                                     as rut,
		--coalesce(bi.single_commision, false) 	  as single_commission,
		--bi.is_debtor,
		--bi.contract_number,
		--bi.sap_id,
		--rbi.marketing_amount 					  as publicity_cost,
		--rbi.automation_amount 					as automation_cost,
		--rbi.logistics_commission 				as logistics_commission,
		--coalesce(rbi.commission, bi.commission) as commission_restaurant,
	coalesce(mc.food_category_id, 0) 		    as main_cousine_category_id,
	mc.food_category_name                       as main_cousine_category_name,
    concept.id                                  as concept_id,
    kitchen.id                                  as kitchen_id,
    p.businessCategory.id                       as business_category_id,
    p.requiresProofOfDelivery                   as requires_proof_of_delivery
FROM `peya-data-origins-stg.origins_data_stg.sqs_partner` p
    left join `peya-data-origins-stg.origins_data_raw.enum_partner_business_type` as pbt on p.businessType = pbt.name
    left join `peya-data-origins-stg.origins_data_raw.enum_partner_delivery_type` as pdt on p.deliveryType = pdt.name
    left join `peya-data-origins-stg.origins_data_raw.enum_partner_stamps_state` as pss on pss.name = p.stampsState
    left join `peya-data-origins-stg.origins_data_raw.enum_partner_state`  as ps on ps.name = p.state
    left join `peya-data-origins-stg.origins_data_raw.enum_partner_type` as pt on pt.name = p.type
    --left join `peya-data-origins-stg.dwh_data.billinginfo` as bi on p.billingInfo.id = bi.id
	--left join `peya-data-origins-stg.dwh_data.partners_billinginfo` as rbi on p.billingInfo.id = rbi.partner_id
	left join partner_food_categories mc on mc.partner_id = p.id
                                                and mc.rank_food_category = 1
where yyyymmdd = FILTER_DATE;