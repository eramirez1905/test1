SELECT country_code
  , zone_id
  , n_orders
  {%- if params.daily or params.backfill %}
  , created_date
  , created_at
  {%- endif %}
FROM `{{ params.project_id }}.{{ params.feature_store_dataset }}.{{ params.view_name }}`
{%- if params.daily %}
-- Note: we do not add where type = 'live_orders' because it uses the live order daily view, which only contains live order data.
WHERE created_date = '{{ ds }}'
{%- endif %}

