UPDATE dim_restaurant
SET qty_products = x.qty_products
	, qty_picts = x.qty_picts
	, qty_custom_picts = x.qty_custom_picts
	, has_custom_photo_menu = (x.qty_custom_picts > 0)
FROM
(
SELECT r.restaurant_id,
		ISNULL(count(distinct p.product_id),0) as qty_products,
		ISNULL(sum(case when (p.product_image <> '-' and p.product_image IS NOT NULL) then 1 else 0 end),0) as qty_picts,
		ISNULL(sum(case when ((p.product_image <> '-' and p.product_image IS NOT NULL) and p.has_custom_photo) then 1 else 0 end),0) as qty_custom_picts
	FROM public.dim_restaurant r
	INNER JOIN public.dim_restaurant_product as pr on pr.restaurant_id = r.restaurant_id  and pr.menu_id = r.menu_id
	AND pr.is_deleted = false
	INNER JOIN peyabi.public.dim_product as p ON p.product_id = pr.product_id
	AND p.is_deleted=FALSE
	AND p.section_is_deleted = false
	AND p.category_is_deleted = false
	GROUP BY 1
) as x
WHERE x.restaurant_id = dim_restaurant.restaurant_id;