select r.restaurant_id
		,coalesce(sum((left(from_date,7) <= left(getdate(),7)
				and ("to_date" is null 
						or (left("to_date",7) >= left(getdate(),7) 
								and left("to_date",10) > left(getdate(),7) + '-01'
							)
					)
				and product_id = 2)::int),0) as qty_current_feature_product
		,coalesce(bool_or(left(from_date,7) <= left(getdate(),7)
				and ("to_date" is null 
						or (left("to_date",7) >= left(getdate(),7) 
								and left("to_date",10) > left(getdate(),7) + '-01'
							)
					)
				and product_id = 2), false) as has_featured_product
		,coalesce(sum((left(from_date,7) <= left(getdate(),7)
					and ("to_date" is null 
							or (left("to_date",7) >= left(getdate(),7) 
									and left("to_date",10) > left(getdate(),7) + '-01'
							   )
						)
					and product_id = 1)::int),0) as qty_current_gold_vip
		,coalesce(bool_or(left(from_date,7) <= left(getdate(),7)
					and ("to_date" is null 
							or (left("to_date",7) >= left(getdate(),7) 
									and left("to_date",10) > left(getdate(),7) + '-01'
							   )
						)
					and product_id = 1),false) as is_gold_vip
from public.dim_restaurant r
		LEFT JOIN public.fact_billing_products fbp on r.restaurant_id = fbp.restaurant_id
group by r.restaurant_id;