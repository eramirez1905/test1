UPDATE dim_restaurant
set last_heartbeat_primary_rs = x.last_heartbeat_primary_rs
    ,last_heartbeat_secondary_rs = x.last_heartbeat_secondary_rs
FROM (
	select
		r.restaurant_id,
		max(s_primary.last_timestamp) as last_heartbeat_primary_rs,
		max(s_secondary.last_timestamp) as last_heartbeat_secondary_rs
	from dim_restaurant r
	    left join public.fact_reception_events_heartbeats s_primary on s_primary.partner_id = r.restaurant_id
	                                                                        and s_primary.reception_system_id = r.orders_reception_system_id
	    left join public.fact_reception_events_heartbeats s_secondary on s_secondary.partner_id = r.restaurant_id
	                                                                        and s_secondary.reception_system_id = r.orders_secondary_reception_system_id
	where s_primary.partner_id is not null
	            or s_secondary.partner_id is not null
	group by 1
) x
WHERE dim_restaurant.restaurant_id = x.restaurant_id;