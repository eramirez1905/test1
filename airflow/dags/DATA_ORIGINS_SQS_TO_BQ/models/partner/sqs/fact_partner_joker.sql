UPDATE public.dim_restaurant
set has_active_joker = false;
/*Actualiza Partners con Jokers activos*/

UPDATE public.dim_restaurant
SET has_active_joker = true
FROM (
SELECT to_char(x.start_date, 'yyyyMMdd')::integer as date_id, x.restaurant_unique_key as partner_id
FROM public.fact_partner_joker x
WHERE to_char(x.start_date, 'yyyyMMdd') = to_char(DATEADD(DAY,-1,CURRENT_DATE), 'yyyyMMdd')
            AND x.enabled is true) joker
WHERE joker.partner_id = dim_restaurant.restaurant_id