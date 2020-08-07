update dim_restaurant
set rd_migration = x.registered_date
, registered_date_id = to_char(x.registered_date,'YYYYMMDD')::integer
,first_date_online = x.registered_date
,first_order_date = x.registered_date
from (
		select 	o.peya_restaurant_id,
				min(o.order_timestamp) as registered_date
		from migrations.rd_ftw_admin_orders o
		where peya_order_state=1
		group by 1
) x
where x.peya_restaurant_id = dim_restaurant.restaurant_id
and dim_restaurant.country_id = 18;