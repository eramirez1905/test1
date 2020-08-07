update public.dim_restaurant
set last_confirmed_order_registered_date = orders.registered_date
from (
	select o.restaurant_id, max(o.registered_date) as registered_date
	from public.fact_orders o
	where o.registered_date >= dateadd(month,-24,getdate())
	and o.state_id = 1
	group by 1
) orders
where orders.restaurant_id = dim_restaurant.restaurant_id;

------------------------------
--dim_historical_restaurant
------------------------------
update public.dim_historical_restaurant
set last_confirmed_order_registered_date = orders.registered_date
from (
	select o.restaurant_id, max(o.registered_date) as registered_date
	from public.fact_orders o
	where o.registered_date >= dateadd(month,-24,getdate())
	and o.state_id = 1
	group by 1
) orders
where orders.restaurant_id = dim_historical_restaurant.restaurant_id
and dim_historical_restaurant.date_id = to_char(dateadd(day,-1,getdate()), 'yyyyMMdd')::integer ;