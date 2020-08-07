SELECT r.restaurant_id
		, COALESCE(avg(rr.speed),0) 			as avg_rating_speed
		, COALESCE(avg(rr.general_score),0) 	as avg_rating
		, COALESCE(avg(rr.food),0)				as avg_rating_food
		, COALESCE(avg(rr.service),0) 			as avg_rating_service
		, COALESCE(count(distinct case 
									when state = 'confirmed'
											and rr.review_description != ''
											and rr.review_description is not null then rr.restaurant_review_id
								  end),0) 		as qty_comments
from public.dim_restaurant r
		LEFT JOIN public.fact_restaurant_reviews rr on r.restaurant_id = rr.restaurant_id
where rr.application_id = 1
GROUP BY 1 


select distinct state
from public.fact_restaurant_reviews
--where review_description > ''
limit 10