SELECT
     restaurant_id
	,replace(trim(restaurant),'%',' ') as restaurant
	,coalesce(area_id,0) as area_id
	,coalesce(city_id,0) as city_id
	,coalesce(country_id,0) as country_id
	,STATE
	,type
	,coalesce(is_vip,false) as is_vip
	,coalesce(migration_id,0) as migration_id
	,coalesce((TIMESTAMP 'epoch' + registered_date/1000 *INTERVAL '1 second'),'1900-01-01'::TIMESTAMP) as registered_date
	,coalesce(REPLACE((TIMESTAMP 'epoch' + registered_date/1000 *INTERVAL '1 second')::date::varchar,'-',''),'19000101')::integer as registered_date_id
	,coalesce((TIMESTAMP 'epoch' + date_created/1000 *INTERVAL '1 second'),'1900-01-01'::TIMESTAMP) as date_created
	,coalesce(REPLACE((TIMESTAMP 'epoch' + date_created/1000 *INTERVAL '1 second')::date::varchar,'-',''),'19000101')::varchar as date_created_id
	,coalesce(is_important_account,false) as is_important_account
	,replace(street,'%',' ') as street
	,replace(door_number,'%',' ') as door_number
	,replace(phone,'%',' ') as phone
	,replace(conctact_name,'%',' ') as conctact_name
	,replace(contact_last_name,'%',' ') as contact_last_name
	,replace(conctact_phones,'%',' ') as conctact_phones
	,has_online_payment
	,has_loyalty_program
	,reception_system_name
	,has_pos
	,main_cousine
	,cant_com
	,talent
	,has_banner
	,has_discount
	,delivery_time
	,min_delivery_amount
	,max_delivery_amount
	,min_shipping_amount
	,max_shipping_amount
	,cantidad_zonas
	,coalesce(delivery_time_id,0) as delivery_time_id
	,coalesce(has_mov,0) as has_mov
	,use_live_order_tracking
	,coalesce(address_id,0) as address_id
	,disabled_reason
	,disabled_motive
	,coalesce(billing_info_id,0) as billing_info_id
	,coalesce(main_cousine_category_id,0) as main_cousine_category_id
	,link
	,logo
	,reception_system_enabled
	,coalesce(publicity_cost) as publicity_cost
	,coalesce(automation_amount) as automation_amount
	,has_shipping_amount
	,coalesce(orders_reception_system_id,0) as orders_reception_system_id
	,coalesce(orders_secondary_reception_system_id,0) as orders_secondary_reception_system_id
	,coalesce(logistics_commission) as logistics_commission
	,delivery_type
	,business_type
	,accepts_vouchers
	,is_online
	,is_offline
	,is_new_registered
	,shipping_amount
	,shipping_amount_is_percentage
	,salesforce_id
	,coalesce(branch_parent_id,0) as branch_parent_id
	,affected_by_porygon_events
	,affected_by_porygon_optimizations
	,replace(public_phone,'%',' ') as public_phone
	,replace(automatic_phone,'%',' ') as automatic_phone
	,(TIMESTAMP 'epoch' + last_updated/1000 *INTERVAL '1 second') as last_updated
	,parallel_reception_system
	,coalesce(menu_id,0) as menu_id
	,accepts_and_supports_vouchers
	,declines_but_supports_vouchers
	,coalesce(kitchen_id,0) as kitchen_id
	,coalesce(concept_id,0) as concept_id
	,coalesce(business_category_id,0) as business_category_id
	,coalesce(accepts_pre_order,false) as accepts_pre_order
	,decode(requires_proof_of_delivery,1,true,false) as requires_proof_of_delivery
	,coalesce(is_darkstore,false) as is_darkstore
	,coalesce(identity_card_behaviour,0) as identity_card_behaviour
	,(single_commission::integer = 1) as single_commission
	,shopper_type
	,coalesce(global_catalog_id,0) as global_catalog_id
	,coalesce(capacity_check,false) as capacity_check
FROM s3.peyadb_partners