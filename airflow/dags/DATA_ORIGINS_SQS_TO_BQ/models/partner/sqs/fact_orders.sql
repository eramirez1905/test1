select o.restaurant_id
		, max(o.registered_date) 														as last_order_date
		, min(o.registered_date) 														as first_order_date
		, BOOL_OR(o.registered_date >= date_trunc('month', current_date - interval '1 day')
						and o.dispatch_date IS NOT NULL) 								as dispatch_current_month
		, max(o.registered_date)::date = (current_date - interval '1 day')::date 		as is_active
from public.fact_orders o
where o.registered_date >= dateadd(month,-24,getdate())
	        and o.state_id = 1
	        --and o.restaurant_id = 60762
group by 1;

/*select *
from dim_restaurant dr 
where is_active
limit 10*/

/*
60762	Iberpark - Costa Urbana
134023	Don Burgers
134548	Ándale Ándale - Columbia Market
163846	Pizzetas Y Mas
144415	Grido Colón
112014	Food & Love Prado
153992	Socrates Bar
49899	Lapana - Casa de Comidas
 */
