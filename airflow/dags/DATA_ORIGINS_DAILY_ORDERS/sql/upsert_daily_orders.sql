--ACTUALIZA LAS ORDENES ANTERIORES
update testing.fact_orders_sqs  set
  restaurant_id=uo.restaurant_id
 ,area_id=uo.area_id
  ,delivery_date=uo.delivery_date
 ,delivery_date_id=uo.delivery_date_id
 ,registered_date=uo.registered_date
 ,registered_date_id=uo.registered_date_id
 ,order_hour=uo.order_hour
 ,is_pre_order =uo.is_pre_order
 ,is_take_out =uo.is_take_out
 ,coupon_used =uo.coupon_used
 ,user_id=uo.user_id
 ,response_user_id=uo.response_user_id
 ,restaurant_user_id=uo.restaurant_user_id
 ,response_date=uo.response_date
 ,response_date_id=COALESCE(uo.response_date_id,19000101)
 ,reject_message_id=uo.reject_message_id
 ,delivery_time_id=uo.delivery_time_id
 ,reception_system_id=uo.reception_system_id
 ,first_successful =uo.first_successful
 ,payment_method_id=uo.payment_method_id
 ,amount_no_discount =uo.payment_method_id
 ,commission =uo.commission
 ,discount =uo.discount
 ,shipping_amount =uo.shipping_amount
 ,total_amount =uo.total_amount
 ,payment_amount =uo.payment_amount
 ,tax_amount =uo.tax_amount
 ,online_payment =uo.online_payment
 ,responded_system_id=uo.responded_system_id
 ,cop_user_id=uo.cop_user_id
 ,user_address =uo.user_address
 ,user_phone =uo.user_phone
 ,white_label_id=uo.white_label_id
 ,address_id=uo.address_id
 ,with_logistics =uo.with_logistics
 ,dispatch_date=uo.dispatch_date
 ,logistics_commission =uo.logistics_commission
 ,promised_delivery_time_id=uo.promised_delivery_time_id
 ,secondary_reception_system_id=uo.secondary_reception_system_id
 ,client_guid =uo.client_guid
 ,latitude=uo.latitude
 ,longitude=uo.longitude
 ,delivery_zone_id=uo.delivery_zone_id
 ,business_type=uo.business_type_id
 ,user_identity_card =uo.user_identity_card
 ,application_version =uo.application_version
 ,shipping_amount_no_discount=uo. shipping_amount_no_discount
 ,credit_card_commission =uo.credit_card_commission
 ,country_id=uo.country_id
 ,city_id=uo.city_id
 ,has_notes =uo.has_notes
 ,restaurant_name =uo.restaurant_name
 ,order_notes =uo.order_notes
 ,has_final_user_documents =uo.has_final_user_documents
 ,has_wallet_credit =uo.has_wallet_credit
 ,distance_kilometers =uo.distance_kilometers
 ,distance_meters =uo.distance_meters
 ,discount_amount =uo.discount_amount
 ,total_amount_with_dc =uo.total_amount_with_dc
 ,commission_amount =uo.commission_amount
 ,commission_includes_delivery_cost =uo.commission_includes_delivery_cost
 ,discount_paid_by_company =uo.discount_paid_by_company
 ,state_id=uo.state_id
 ,application_id=uo.application_id
from s3.upsert_orders uo
where testing.fact_orders_sqs.order_id=uo.order_id;

--INSERTA LAS ORDENES NUEVAS
insert into testing.fact_orders_sqs(
rejection_call_date_id
,rejection_call_user_id
,order_id
, restaurant_id
 ,area_id
  ,delivery_date
 ,delivery_date_id
 ,registered_date
 ,registered_date_id
 ,order_hour
 ,is_pre_order
 ,is_take_out 
 ,coupon_used
 ,user_id
 ,response_user_id
 ,restaurant_user_id
 ,response_date
 ,response_date_id
 ,reject_message_id
 ,delivery_time_id
 ,reception_system_id
 ,first_successful
 ,payment_method_id
 ,amount_no_discount
 ,commission
 ,discount
 ,shipping_amount
 ,total_amount
 ,payment_amount
 ,tax_amount
 ,online_payment
 ,responded_system_id
 ,cop_user_id
 ,user_address
 ,user_phone
 ,white_label_id
 ,address_id
 ,with_logistics
 ,dispatch_date
 ,logistics_commission
 ,promised_delivery_time_id
 ,secondary_reception_system_id
 ,client_guid
 ,latitude
 ,longitude
 ,delivery_zone_id
 ,business_type
 ,user_identity_card 
 ,application_version
 ,shipping_amount_no_discount
 ,credit_card_commission
 ,country_id
 ,city_id
 ,has_notes
 ,restaurant_name 
 ,order_notes 
 ,has_final_user_documents
 ,has_wallet_credit
 ,distance_kilometers
 ,distance_meters 
 ,discount_amount 
 ,total_amount_with_dc
 ,commission_amount 
 ,commission_includes_delivery_cost
 ,discount_paid_by_company
 ,state_id
 ,application_id
)
select 
19000101
,0
,uo.order_id
,uo.restaurant_id
 ,uo.area_id
  ,uo.delivery_date
 ,uo.delivery_date_id
 ,uo.registered_date
 ,uo.registered_date_id
 ,uo.order_hour
 ,uo.is_pre_order
 ,uo.is_take_out 
 ,uo.coupon_used
 ,uo.user_id
 ,uo.response_user_id
 ,uo.restaurant_user_id
 ,uo.response_date
 ,COALESCE(uo.response_date_id,19000101)
 ,uo.reject_message_id
 ,uo.delivery_time_id
 ,uo.reception_system_id
 ,uo.first_successful
 ,uo.payment_method_id
 ,uo.amount_no_discount
 ,uo.commission
 ,uo.discount
 ,uo.shipping_amount
 ,uo.total_amount
 ,uo.payment_amount
 ,uo.tax_amount
 ,uo.online_payment
 ,uo.responded_system_id
 ,uo.cop_user_id
 ,uo.user_address
 ,uo.user_phone
 ,uo.white_label_id
 ,uo.address_id
 ,uo.with_logistics
 ,uo.dispatch_date
 ,uo.logistics_commission
 ,uo.promised_delivery_time_id
 ,uo.secondary_reception_system_id
 ,uo.client_guid
 ,uo.latitude
 ,uo.longitude
 ,uo.delivery_zone_id
 ,uo.business_type_id
 ,uo.user_identity_card 
 ,uo.application_version
 ,uo.shipping_amount_no_discount
 ,uo.credit_card_commission
 ,uo.country_id
 ,uo.city_id
 ,uo.has_notes
 ,uo.restaurant_name 
 ,uo.order_notes 
 ,uo.has_final_user_documents
 ,uo.has_wallet_credit
 ,uo.distance_kilometers
 ,uo.distance_meters 
 ,uo.discount_amount 
 ,uo.total_amount_with_dc
 ,uo.commission_amount 
 ,uo.commission_includes_delivery_cost
 ,uo.discount_paid_by_company
 ,uo.state_id
 ,uo.application_id
 from s3.upsert_orders as uo
 left join testing.fact_orders_sqs fo on fo.order_id=uo.order_id
 where fo.order_id is null