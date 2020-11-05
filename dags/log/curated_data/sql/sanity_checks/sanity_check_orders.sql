SELECT count(*) = 0 AS entity_id_null
FROM `{{ params.project_id }}.cl.orders`
WHERE entity.id IS NULL
