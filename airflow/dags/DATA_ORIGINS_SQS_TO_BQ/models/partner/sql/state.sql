UPDATE dim_restaurant
SET
previous_state_id=query.previous_state_id,
change_state_date_id=query.change_state_date_id
FROM
(
		 --Obtengo el ultimo estado del restaurante anterior al actual
		select distinct r.restaurant_id,r.restaurant_state_id as previous_state_id, max(date_id) as change_state_date_id
		from public.dim_historical_restaurant r
		join (
		--Obtengo el ultimo estado del restaurante anterior al actual
		select distinct restaurant_id,  last_value(restaurant_state_id)
		over(partition by restaurant_id order by date_id asc
		rows between unbounded preceding and unbounded following)
		from (
				select r.date_id, r.restaurant_id, r.restaurant_state_id
				from public.dim_historical_restaurant r
				join public.dim_restaurant d on r.restaurant_id=d.restaurant_id
				where r.restaurant_state_id<>0 and r.restaurant_state_id <> d.restaurant_state_id)
		order by restaurant_id
		 ) a
		 on a.restaurant_id=r.restaurant_id and a.last_value=r.restaurant_state_id
		 group by r.restaurant_id,r.restaurant_state_id
 ) query
 WHERE query.restaurant_id=dim_restaurant.restaurant_id