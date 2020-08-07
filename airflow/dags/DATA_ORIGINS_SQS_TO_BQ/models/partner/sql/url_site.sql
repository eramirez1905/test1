UPDATE dim_restaurant
SET url_site = x.url_site
FROM
(
	SELECT r.restaurant_id, cr.url_peya||'restaurantes/'||c.slug||'/'||r.link||'-menu' as url_site
	FROM public.dim_restaurant r
	INNER JOIN public.dim_city c
		ON r.city_id = c.city_id
	INNER JOIN public.dim_country_details cr
		ON cr.country_id = c.country_id
) as x
WHERE x.restaurant_id = dim_restaurant.restaurant_id;