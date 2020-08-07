select r.restaurant_id,
       coalesce(bool_or((w.discount_value_to_date is null
            		or w.discount_value_to_date > (CURRENT_DATE + interval '4 hour')
            	) and w.discount_value > 0), false) as has_discount
from public.dim_restaurant r
		LEFT JOIN public.dim_restaurant_weekly_discount w on r.restaurant_id = w.restaurant_id
																and w.discount_value_from_date <= (CURRENT_DATE + interval '4 hour')
group by 1


/*select *
from public.dim_restaurant_weekly_discount
limit 10*/

