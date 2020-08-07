SELECT r.restaurant_id,
	   coalesce(count(pl.restaurant_id),0) > 0 as has_restaurant_portal
FROM public.dim_restaurant r
		LEFT JOIN public.fact_restaurant_portal_login pl ON r.restaurant_id = pl.restaurant_id
group by 1