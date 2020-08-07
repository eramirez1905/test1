drop TABLE IF EXISTS s3.nrt_partner_tags;
create EXTERNAL TABLE s3.nrt_partner_tags(
    restaurant_id bigint,
    "tag" varchar(500)
)
STORED AS parquet
LOCATION 's3://peyabi.datalake.live/bi-core-tech/stg/partner/tags/';

select *
from s3.nrt_partner_tags_ods
limit 100;

drop TABLE IF EXISTS s3.nrt_partner_channels;
create EXTERNAL TABLE s3.nrt_partner_channels(
    restaurant_id bigint,
    channel_id bigint,
    channel_slug varchar(500)
)
STORED AS parquet
LOCATION 's3://peyabi.datalake.live/bi-core-tech/stg/partner/channels/';

select *
from s3.nrt_partner_channels
limit 100;


drop TABLE IF EXISTS s3.nrt_partner_areas;
create EXTERNAL TABLE s3.nrt_partner_areas(
    restaurant_id bigint,
    area_id bigint
)
STORED AS parquet
LOCATION 's3://peyabi.datalake.live/bi-core-tech/stg/partner/areas/';

select *
from s3.nrt_partner_areas
limit 100;


drop TABLE IF EXISTS s3.nrt_partner_food_categories;
create EXTERNAL TABLE s3.nrt_partner_food_categories(
     restaurant_id bigint,
     restaurant_food_category_id bigint,
     enabled boolean,
	 food_category_id bigint,
	 food_category_name varchar(500),
	 food_category_isdeleted boolean,
	 food_category_visible boolean,
	 food_category_country_id bigint,
	 manually_sorted boolean,
	 percentage double precision,
	 quantity bigint,
	 sorting_index bigint,
	 state varchar(150)
)
STORED AS parquet
LOCATION 's3://peyabi.datalake.live/bi-core-tech/stg/partner/food_categories/';

select *
from s3.nrt_partner_food_categories
limit 100;

drop TABLE IF EXISTS s3.nrt_partner_payment_methods;
create EXTERNAL TABLE s3.nrt_partner_payment_methods(
    restaurant_id bigint,
    payment_method_id bigint,
    payment_method_online boolean
)
STORED AS parquet
LOCATION 's3://peyabi.datalake.live/bi-core-tech/stg/partner/payment_methods/';

select *
from s3.nrt_partner_payment_methods
limit 100;

/*drop table s3.enum_partner_state;
create external table s3.enum_partner_state
(
	"ID" integer,
	"NAME" varchar(150)
)
row format delimited 
fields terminated by ','
stored as textfile
location 's3://peyabi.datalake.live/bi-core-tech/raw/nrt/entidades/test/'
table properties ('skip.header.line.count'='1');

select id, name
from s3.enum_partner_state
*/

drop TABLE IF EXISTS s3.nrt_partner;
create EXTERNAL TABLE s3.nrt_partner(
    		accepts_pre_order boolean,
			accepts_vouchers boolean,
			city_id bigint,
			area_id bigint,
			restaurant_door_number varchar(max),
			address_id bigint,
			restaurant_phone varchar(max),
			restaurant_street varchar(max),
			affected_by_porygon_events boolean,
			affected_by_porygon_optimizations boolean,
			automatic_phone varchar(max),
			automatic_phone_enabled boolean,
			billing_info_id bigint,
			branch_id bigint,
			branch_name varchar(max),
			business_name varchar(max),
			business_type varchar(max),
			capacity_check boolean,
			country_id bigint,
			country_url_site varchar(max),
			created_date_id integer,
			created_date timestamp,
			delivery_time_description varchar(max),
			delivery_time_id bigint,
			delivery_time_max_minutes bigint,
			delivery_time_min_minutes bigint,
			delivery_time varchar(max),
			delivery_time_order bigint,
			delivery_type varchar(max),
			description varchar(max),
			disabled_reason varchar(max),
			header_image varchar(max),
			home_vip boolean,
			restaurant_id bigint,
			identity_card_behaviour varchar(max),
			integration_code varchar(max),
			integration_name varchar(max),
			is_darkstore boolean,
			is_important_account boolean,
			is_vip boolean,
			last_updated timestamp,
			link varchar(max),
			logo varchar(max),
			mandatory_address_confirmation boolean,
			mandatory_identity_card boolean,
			mandatory_payment_amount boolean,
			max_shipping_amount double precision,
			menu_id bigint,
			menu_name varchar(max),
			message_id varchar(max),
			message_timestamp bigint,
			backend_id bigint,
			min_delivery_amount double precision,
			restaurant_name varchar(max),
			restaurant_noindex boolean,
			restaurant_noindex_google_places boolean,
			orders_reception_system_id bigint,
			has_pos boolean,
			reception_system_name varchar(max),
			orders_secondary_reception_system_id bigint,
			orders_secondary_reception_system_ispos boolean,
			orders_secondary_reception_system_name varchar(max),
			parallel_reception_system boolean,
			has_online_payment boolean,
			private_phone varchar(max),
			public_phone varchar(max),
			reception_system_enabled boolean,
			registered_date timestamp,
			registered_date_id integer,
			is_new_registered boolean,
			is_chain boolean,
			centralized_reception_partner_id bigint,
			restaurant_brand_name varchar(max),
			restaurant_trust_score_id bigint,
			salesforce_id varchar(max),
			shipping_amount double precision,
			has_shipping_amount boolean,
			shipping_amount_is_percentage boolean,
			stamps_needed bigint,
			has_stamps boolean,
			stamps_state varchar(max),
			restaurant_state varchar(max),
			is_pending boolean,
			is_offline boolean,
			is_online boolean,
			is_premium boolean,
			is_express boolean,
			is_talent boolean,
			restaurant_type varchar(max),
			is_logistic boolean,
			restaurant_type_id integer,
			restaurant_state_id integer,
			business_type_id integer,
			delivery_type_id integer,
			stamps_state_id integer
)
STORED AS parquet
LOCATION 's3://peyabi.datalake.live/bi-core-tech/stg/partner/partner/';


select *
from s3.nrt_partner
where is_premium
limit 100;

drop table testing.nrt_partner;

create table testing.nrt_partner AS
select   p.accepts_pre_order
		,p.accepts_vouchers
		,p.city_id
		,p.area_id
		,p.restaurant_door_number
		,p.address_id
		,p.restaurant_phone
		,p.restaurant_street
		,p.affected_by_porygon_events
		,p.affected_by_porygon_optimizations
		,p.automatic_phone
		,p.automatic_phone_enabled
		,p.billing_info_id
		,p.branch_id
		,p.branch_name
		,p.business_name
		--,p.business_type
		,p.capacity_check
		,p.country_id
		,p.country_url_site
		,p.created_date_id
		,p.created_date
		,p.delivery_time_description
		,p.delivery_time_id
		,p.delivery_time_max_minutes
		,p.delivery_time_min_minutes
		,p.delivery_time
		,p.delivery_time_order
		,p.delivery_type
		,p.description
		,p.disabled_reason
		,p.header_image
		,p.home_vip
		,p.restaurant_id
		,p.identity_card_behaviour
		,p.integration_code
		,p.integration_name
		,p.is_darkstore
		,p.is_important_account
		,p.is_vip
		,p.last_updated
		,p.link
		,p.logo
		,p.mandatory_address_confirmation
		,p.mandatory_identity_card
		,p.mandatory_payment_amount
		,p.max_shipping_amount
		,p.menu_id
		,p.menu_name
		,p.message_id
		,p.message_timestamp
		,p.backend_id
		,p.min_delivery_amount
		,p.restaurant_name
		,p.restaurant_noindex
		,p.restaurant_noindex_google_places
		,p.orders_reception_system_id
		,p.has_pos
		,p.reception_system_name
		,p.orders_secondary_reception_system_id
		,p.orders_secondary_reception_system_ispos
		,p.orders_secondary_reception_system_name
		,p.parallel_reception_system
		,p.has_online_payment
		,p.private_phone
		,p.public_phone
		,p.reception_system_enabled
		,p.registered_date
		,p.registered_date_id
		,p.is_new_registered
		,p.is_chain
		,p.centralized_reception_partner_id
		,p.restaurant_brand_name
		,p.restaurant_trust_score_id
		,p.salesforce_id
		,p.shipping_amount
		,p.has_shipping_amount
		,p.shipping_amount_is_percentage
		,p.stamps_needed
		,p.has_stamps
		,p.stamps_state
		,p.restaurant_state
		,p.is_pending
		,p.is_offline
		,p.is_online
		,p.is_premium
		,p.is_express
		,p.is_talent
		,p.restaurant_type
		,p.is_logistic
		,p.restaurant_type_id
		,p.restaurant_state_id
		,p.business_type_id AS business_type
		,p.delivery_type_id
		,p.stamps_state_id
		,bi.company_number 						as rut
		,bi.commission
		,coalesce(bi.single_commission, false) 	as single_commission
		,bi.is_debtor
		,coalesce(bi.migration_id, 0) 			as contract_number
		,bi.sap_id
		,rbi.marketing_amount 					as publicity_cost
		,rbi.automation_amount 					as automation_cost
		,rbi.logistics_commission 				as logistics_commission
		,coalesce(rbi.commission, bi.commission) as commission_restaurant
		,coalesce(mc.food_category_id, 0) 		 as main_cousine_category_id
		,mc.main_cousine
from s3.nrt_partner p
		left join s3.peyadb_billing_info bi on p.billing_info_id = bi.id
		left join s3.peyadb_restaurant_billing_info rbi on p.restaurant_id = rbi.restaurant_id
		left join (select 
							  restaurant_id
							, food_category_id
							, food_category_name AS main_cousine
							, ROW_NUMBER() over(partition by restaurant_id 
												order by sorting_index asc
															, percentage) as rank_food_category
							, sorting_index
							, percentage
					from s3.nrt_partner_food_categories fc
					where enabled 
							and state in ('AUTO_ON', 'MANUAL_ON', 'BI_ON')
					) mc on mc.restaurant_id = p.restaurant_id and mc.rank_food_category = 1
limit 100;

select *
from s3.peyadb_billing_info
limit 10


select restaurant_id
		, food_category_id
		, "name" AS main_cousine
		,ROW_NUMBER() over(partition by restaurant_id 
							order by sorting_index asc
										, percentage) as rank_food_category
from s3.nrt_partner_food_categories fc
where enabled = 1 
		and state in (1, 3, 5)
		and restaurant_id = 40699
order by sorting_index asc, percentage desc

select column_prod, column_sqs
from 
	(
	select column_name 			as column_prod
			, ordinal_position 	as column_ordinal
	from information_schema.columns
	where table_name = 'dim_restaurant'
			and table_schema = 'public'
			and column_name not like 'audi_%'
	) c1
left join 	
	(
	select column_name 		   as column_sqs
	from information_schema.columns
	where table_name = 'nrt_partner'
			and table_schema = 'testing'
	) c2 on c1.column_prod = c2.column_sqs
where true 
		--and c2.column_sqs is null
		--and c1.column_prod like '%_id%'
order by column_ordinal asc

fact


select distinct restaurant_state_id
from dim_restaurant dr

select *
from dim_restaurant_state


drop TABLE IF EXISTS s3.nrt_partner_raw;
create EXTERNAL TABLE s3.nrt_partner_raw(
    "acceptsPreOrder" boolean,
    "acceptsVouchers" boolean,
    address struct<area:struct<city:struct<id:bigint,name:varchar(5000)>,id:bigint,name:varchar(5000)>,complement:varchar(5000), corner:varchar(5000),doorNumber:varchar(5000),id:bigint, isDeleted:boolean,latitude:double precision,longitude:double precision,notes:varchar(5000),phone:varchar(5000),street:varchar(5000),zipCode:varchar(5000)>,
    "affectedByPorygonEvents" boolean,
    "affectedByPorygonOptimizations" boolean,
    areas array<struct<id:bigint>>,
    "automaticPhone" varchar(5000),
    "automaticPhoneEnabled" boolean,
    billingInfo struct<id:bigint>,
    branchParent struct<id:bigint,name:varchar(5000)>,
    businessType varchar(5000),
    capacityCheck boolean,
    channels array<struct<id:bigint,slug:varchar(5000)>>,
    country struct<culture:varchar(5000),currency:struct<id:bigint,isoCode:varchar(5000),symbol:varchar(5000)>,id:bigint,isEnabled:boolean,name:varchar(5000),phonePrefix:varchar(5000),shortName:varchar(5000),timeOffset:bigint,timeZone:varchar(5000),url:varchar(5000)>,
    dateCreated timestamp,
    deliveryTime struct<description:varchar(5000),id:bigint,maxMinutes:bigint,minMinutes:bigint,name:varchar(5000),"order":bigint>,
    deliveryType varchar(5000),
    description varchar(5000),
    disabledReason varchar(5000),
    foodCategories array<struct<enabled:boolean,foodCategory:struct<country:struct<id:bigint>,foodCategoryImages:array<struct<id:bigint,name:varchar(5000)>>,id:bigint,isDeleted:boolean,name:varchar(5000),visible:boolean>, id:bigint,manuallySorted:boolean,percentage:double precision,quantity:bigint,sortingIndex:bigint,state:varchar(5000)>>,
    "hash" varchar(5000),
    headerImage varchar(5000),
    homeVip boolean,
    id bigint,
    identityCardBehaviour varchar(5000),
    integrationCode varchar(5000),
    integrationName varchar(5000),
    isDarkstore boolean,
    isImportantAccount boolean,
    isVip boolean,
    lastUpdated timestamp,
    link varchar(5000),
    logo varchar(5000),
    mandatoryAddressConfirmation boolean,
    mandatoryIdentityCard boolean,
    mandatoryPaymentAmount boolean,
    maxShippingAmount double precision,
    menu struct<id:bigint,"name":varchar(5000)>,
    messageId varchar(5000),
    messageTimestamp bigint,
    migrationId bigint,
    minDeliveryAmount double precision,
    "name" varchar(5000),
    noIndex boolean,
    noIndexGooglePlaces boolean,
    ordersReceptionSystem struct<"category":varchar(5000),id:bigint,isPos:boolean,"name":varchar(5000)>,
    ordersSecondaryReceptionSystem struct<"category":varchar(5000),id:bigint,isPos:boolean,"name":varchar(5000)>,
    parallelReceptionSystem boolean,
    paymentMethods array<struct<id:bigint,"online":boolean>>,
    privatePhone varchar(5000),
    publicPhone varchar(5000),
    receptionSystemEnabled boolean,
    registeredDate timestamp,
    restaurantBrand struct<id:bigint,"name":varchar(5000)>,
    restaurantTrustScore struct<id:bigint>,
    salesforceId varchar(5000),
    shippingAmount double precision,
    shippingAmountIsPercentage boolean,
    stampsNeeded bigint,
    stampsState varchar(5000),
    state varchar(5000),
    tags array<varchar(5000)>,
    talent boolean,
    "type" varchar(5000),
    useLiveOrderTracking boolean
)
STORED AS parquet
LOCATION 's3://peyabi.datalake.live/bi-core-tech/raw/nrt/partner/yyyymmdd=20200529/';

drop table if exists testing.dim_partner_sqs;
create table testing.dim_partner_sqs as
select 
	acceptsPreOrder 				as accepts_pre_order, 
	acceptsVouchers 				as accepts_vouchers, 
	r.address.area.city.id 			as city_id, 
	r.address.area.id 				as area_id, 
	r.address.doorNumber 			as restaurant_door_number, 
	r.address.id 					as address_id, 
	r.address.phone 				as restaurant_phone, 
	r.address.street 				as restaurant_street, 
	affectedByPorygonEvents 		as affected_by_porygon_events, 
	affectedByPorygonOptimizations 	as affected_by_porygon_optimizations, 
	automaticPhone 					as automatic_phone, 
	automaticPhoneEnabled 			as automatic_phone_enabled, 
	r.billingInfo.id 					as billing_info_id, 
	r.branchParent.id is not null 			as is_chain, 
	COALESCE(r.branchParent.id,0)			as branch_id, 
	COALESCE(r.branchParent.name,'NA')		as branch_name, 
	businessType 							as business_name, 
	businessType 							as business_type, 
	capacityCheck 							as capacity_check,
	r.country.id 								as country_id, 
	r.country.url || 'restaurantes/'
				--|| c.slug ||'/'
				|| r.link||'-menu' 			as url_site, 
	to_char(dateCreated,'yyyymmdd')::int	as created_date_id, 
	dateCreated 							as created_date, 
	r.deliveryTime.description 				as delivery_time_description, 
	r.deliveryTime.id 						as delivery_time_id, 
	r.deliveryTime.maxMinutes 				as delivery_time_max_minutes, 
	r.deliveryTime.minMinutes 				as delivery_time_min_minutes, 
	r.deliveryTime."name" 					as delivery_time, 
	r.deliveryTime."order" 					as delivery_time_order, 
	deliveryType 			as delivery_type, 
	description 			as description, 
	disabledReason 			as disabled_reason, 
	/*foodCategories[].id*/0 as main_cousine_category_id, 
	headerImage 			as header_image, 
	homeVip 				as home_vip, 
	r.id 					as restaurant_id, 
	identityCardBehaviour 	as identity_card_behaviour, 
	integrationCode 		as integration_code, 
	integrationName 		as integration_name, 
	isDarkstore 			as is_darkstore, 
	isImportantAccount 		as is_important_account, 
	isVip 					as is_vip, 
	lastUpdated 			as last_updated, 
	link 					as link, 
	logo 					as logo, 
	mandatoryAddressConfirmation 	as mandatory_address_confirmation, 
	mandatoryIdentityCard 			as mandatory_identity_card, 
	mandatoryPaymentAmount 			as mandatory_payment_amount, 
	maxShippingAmount 				as max_shipping_amount, 
	-- as has_banner, 
	r.menu.id 						as menu_id, 
	r.menu."name" 					as menu_name, 
	messageId 						as message_id, 
	messageTimestamp 				as message_timestamp, 
	migrationId 					as backend_id, 
	minDeliveryAmount 				as min_delivery_amount, 
	"name" 							as restaurant_name, 
	noIndex 						as restaurant_noindex, 
	noIndexGooglePlaces 			as restaurant_noindex_google_places, 
	r.ordersReceptionSystem.id 		as orders_reception_system_id, 
	r.ordersReceptionSystem.isPos 	as has_pos, 
	r.ordersReceptionSystem.name 	as reception_system_name, 
	r.ordersSecondaryReceptionSystem.id 		as orders_secondary_reception_system_id, 
	r.ordersSecondaryReceptionSystem.isPos 	as orders_secondary_reception_system_ispos, 
	r.ordersSecondaryReceptionSystem.name 	as orders_secondary_reception_system_name, 
	parallelReceptionSystem 				as parallel_reception_system, 
	/*paymentMethods[].online = true*/false as has_online_payment, 
	privatePhone 							as private_phone, 
	publicPhone 							as public_phone, 
	receptionSystemEnabled 					as reception_system_enabled, 
	registeredDate 							as registered_date, 
	to_char(registeredDate,'yyyymmdd')::int	as registered_date_id, 
	(registeredDate = CURRENT_DATE - interval '1 day') 	as is_new_registered, 
	r.restaurantBrand.id 					as centralized_reception_partner_id, 
	r.restaurantBrand.name 					as restaurant_brand_name, 
	r.restaurantTrustScore.id 				as restaurant_trust_score_id, 
	salesforceId 							as salesforce_id, 
	shippingAmount 							as shipping_amount, 
	shippingAmount > 0 						as has_shipping_amount, 
	shippingAmountIsPercentage 				as shipping_amount_is_percentage, 
	stampsNeeded 							as stamps_needed, 
	stampsNeeded > 0 						as has_stamps,
	state 									as restaurant_state_id, 
	state = '3' 								as is_pending, 
	state not in ('1', '4') and r."type" <> '3' 	as is_offline, 
	state in ('1', '4') and r."type" = '3' 			as is_online, 
	/*tags[] == '##DELIVERY_PREMIUM'*/ false 	as is_premium, 
	/*tags[] == '##delivery-express'*/ false 	as is_express, 
	talent 									as is_talent, 
	r."type" 								as restaurant_type_id, 
	useLiveOrderTracking 					as is_logistic,
	bi.company_number 						as rut,
	bi.commission,
	bi.is_debtor,
	coalesce(bi.migration_id,0)  			as contract_number,
	bi.sap_id,
	coalesce(bi.single_commission, false) 	as single_commission
from s3.sqs_partner as r
		left join s3.peyadb_billing_info bi on bi.id = r.billingInfo.id
		--left join s3.peyadb_restaurant_billing_info rbi on rbi.restaurant_id = r.id
limit 5


select sp.id, t
from s3.sqs_partner sp
		left join sp.tags t on true
where upper(t) like '%##DELIVERY-EXPRESS%'
--limit 10000


