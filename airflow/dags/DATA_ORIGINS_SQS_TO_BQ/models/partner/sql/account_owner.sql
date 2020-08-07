UPDATE public.dim_restaurant
SET account_owner = null
where dim_restaurant.country_id in (6,18);
UPDATE public.dim_restaurant
SET account_owner = x.account_owner
FROM (
	SELECT x.grid ,
		x.account_owner
	FROM  public.dim_portfolio_by_grid x -- nueva version
) as x
WHERE dim_restaurant.salesforce_id = x.grid
and dim_restaurant.country_id in (6,18);

------------------------------------------------------------------------
--Account Owner not country 6,18
------------------------------------------------------------------------
UPDATE public.dim_restaurant
SET account_owner = null
where dim_restaurant.country_id not in (6,18);
UPDATE public.dim_restaurant
SET account_owner = x.account_owner
FROM (
	SELECT x.backend_id,
		x.account_owner
	FROM  public.dim_portfolio x -- version anterior
) as x
WHERE restaurant_id = x.backend_id
AND dim_restaurant.country_id not in (6,18);