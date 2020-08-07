UPDATE dim_restaurant
SET qty_picts_portal = x.qty_picts_portal,
    qty_products_portal = x.qty_products_portal
FROM
(
	SELECT rp.restaurant_id,
		coalesce(count(distinct p.product_id),0) as qty_products_portal,
		coalesce(sum(case
		                when (p.product_image <> '-'
		                        and p.product_image IS NOT NULL) then 1
		                else 0
		           end),0)                      as qty_picts_portal
	FROM public.dim_restaurant r
	        INNER JOIN public.dim_restaurant_product as rp on rp.restaurant_id = r.restaurant_id
	                                                                and rp.menu_id = r.menu_id
	                                                                AND rp.is_deleted = false
	                                                                AND rp.unavailable is false
            INNER JOIN peyabi.public.dim_product as p ON p.product_id = rp.product_id
                                                            AND p.is_deleted=FALSE
                                                            AND p.section_is_deleted = false
                                                            AND p.category_is_deleted = false
	GROUP BY 1
) as x
WHERE x.restaurant_id = dim_restaurant.restaurant_id;