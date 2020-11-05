SELECT created_date
  , queued_orders_timestamp AS timestamp
  , global_entity_id
  , vendor_id
  , orders_queued
FROM `{{ params.project_id }}.{{ params.feature_store_dataset }}.{{ params.view_name }}`
-- Note: we add where type = 'queued_orders' because it uses the real time view, which contains both live orders and queued orders.
WHERE type = 'queued_orders'
{%- if params.daily %}
  AND CAST(queued_orders_timestamp AS DATE) = '{{ ds }}'
{%- endif %}
