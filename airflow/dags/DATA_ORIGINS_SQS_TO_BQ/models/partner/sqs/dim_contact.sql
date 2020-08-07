select r.restaurant_id
		, conctact_name
		, conctact_phones
		, conctact_email
from public.dim_restaurant r
		LEFT JOIN  (
				select
					c.restaurant_id,
				    c."name"        AS conctact_name,
				    c.last_name  	AS contact_last_name,
				    c.phones     	AS conctact_phones,
				    c.email			as conctact_email,
				    ROW_NUMBER() over(partition by c.restaurant_id order by c.restaurant_contact_id desc) as contact_rank
				from dim_restaurant_contact c
				where type = '1'
						and not is_deleted) as t on r.restaurant_id = t.restaurant_id and t.contact_rank = 1
