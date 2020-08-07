SELECT u.id
	   -- , JSON_OBJECT('id', u.country_id) 					as country
	   -- , f.favorites
	   -- , ua.address
	   , JSON_OBJECT('id', u.id
	   				, 'country', JSON_OBJECT('id', u.country_id)
	   				, 'userTrustScore', JSON_OBJECT('id', uts.id)
	   				, 'restaurant', JSON_OBJECT('id', u.restaurant_id)
	   				, 'favorites', f.favorites
	   				, 'address', ua.address
	   				, 'data', JSON_OBJECT('applicationType', ud.application_type
	   										, 'averageTicket', ud.average_ticket 
	   										, 'birth', ud.birth 
	   										, 'dhUuid', ud.dh_uuid
	   										, 'gender', ud.gender
	   										, 'id', ud.id
	   									 )
	   				) as json_data
FROM peyadb.`user` u
		left join (select user_id, JSON_ARRAYAGG(JSON_OBJECT('id', id)) as favorites 
					from peyadb.favorite
					where user_id < 100000
					group by 1
				  ) f on u.id = f.user_id
		left join (select user_addresses_id, JSON_ARRAYAGG(JSON_OBJECT('id', address_id)) as address
					from peyadb.user_address
					where user_addresses_id < 100000
				    group by 1) as ua on ua.user_addresses_id = u.id
		left join peyadb.user_trust_score uts on uts.user_id = u.id
		left join peyadb.user_data ud on u.id = ud.id 
where u.country_id = 1 
			and u.id < 100000
limit 10