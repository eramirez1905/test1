/*UPDATE dim_restaurant
SET   qty_products = x.qty_products
	, qty_picts = x.qty_picts
	, qty_custom_picts = x.qty_custom_picts
	, has_custom_photo_menu = (x.qty_custom_picts > 0)
FROM
(*/
SELECT r.restaurant_id,
		COALESCE(count(distinct p.product_id),0) 						as qty_products,
		COALESCE(count(distinct case 
								when (p.product_image <> '-' 
										and p.product_image IS NOT NULL) then p.product_id 
							  end),0) 								as qty_picts,
		COALESCE(count(distinct case 
								when ((p.product_image <> '-' 
										and p.product_image IS NOT NULL
									   ) 
									  and p.has_custom_photo
									 ) then p.product_id 
							  end),0) 								as qty_custom_picts
	FROM public.dim_restaurant r
			LEFT JOIN public.dim_restaurant_product as pr on pr.restaurant_id = r.restaurant_id 
																	and pr.menu_id = r.menu_id
																	and pr.is_deleted = false
			LEFT JOIN peyabi.public.dim_product as p ON p.product_id = pr.product_id
																	and p.is_deleted = false
																	and p.section_is_deleted = false
																	and p.category_is_deleted = false
	GROUP BY 1
/*) as x
WHERE x.restaurant_id = dim_restaurant.restaurant_id
*/;