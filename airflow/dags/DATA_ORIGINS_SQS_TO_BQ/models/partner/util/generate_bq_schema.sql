select REPLACE(REPLACE('{"name": "@colName","type": "@dataType"},','@colName', ct.column_name), '@dataType', 
	case cp.data_type
		when 'timestamp without time zone' then 'TIMESTAMP'
		when 'double precision' then 'NUMERIC'
		when 'character varying' then 'STRING'
		when 'bigint' then 'INTEGER'
		else cp.data_type
	end) as bq_schema
	, ct.column_name || ', ', ct.data_type as data_type_nrt, cp.data_type as data_type_prod, ct.data_type = cp.data_type as data_type_check
from information_schema.columns ct
		inner join information_schema.columns cp on ct.column_name = cp.column_name
where ct.table_schema = 'testing'
		and ct.table_name = 'nrt_partner'
		and cp.table_schema = 'public'
		and cp.table_name = 'dim_restaurant'
order by cp.ordinal_position asc

 
select 
	restaurant_id, 
	restaurant_name, 
	restaurant_state_id, 
	restaurant_type_id, 
	area_id, 
	country_id, 
	registered_date_id, 
	backend_id, 
	is_vip, 
	is_important_account, 
	registered_date, 
	created_date_id, 
	created_date, 
	rut, 
	has_online_payment, 
	has_stamps, 
	has_pos, 
	restaurant_street, 
	restaurant_door_number, 
	restaurant_phone, 
	reception_system_name, 
	commission, 
	max_shipping_amount, 
	min_delivery_amount, 
	publicity_cost, 
	automation_cost, 
	main_cousine, 
	is_talent, 
	delivery_time, 
	is_express, 
	is_debtor, 
	contract_number, 
	is_logistic, 
	address_id, 
	is_premium, 
	main_cousine_category_id, 
	link, 
	logo, 
	reception_system_enabled, 
	city_id, 
	disabled_reason, 
	has_shipping_amount, 
	orders_reception_system_id, 
	orders_secondary_reception_system_id, 
	logistics_commission, 
	sap_id, 
	commission_restaurant, 
	delivery_type, 
	business_type, 
	business_name, 
	billing_info_id, 
	is_online, 
	is_offline, 
	accepts_vouchers, 
	is_pending, 
	is_chain, 
	is_new_registered, 
	shipping_amount, 
	shipping_amount_is_percentage, 
	salesforce_id, 
	centralized_reception_partner_id, 
	affected_by_porygon_events, 
	affected_by_porygon_optimizations, 
	public_phone, 
	automatic_phone, 
	last_updated, 
	parallel_reception_system, 
	menu_id, 
	accepts_pre_order, 
	is_darkstore, 
	identity_card_behaviour, 
	single_commission, 
	capacity_check,
	audi_load_date
from public.dim_restaurant dr