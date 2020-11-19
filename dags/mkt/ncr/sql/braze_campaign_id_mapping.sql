TRUNCATE {{ ncr_schema }}.{{ brand_code }}_braze_campaign_id_mapping;

INSERT INTO {{ ncr_schema }}.{{ brand_code }}_braze_campaign_id_mapping (
  brand_code,
  campaign_type,
  braze_campaign_id
)
VALUES
{% for campaign_type, braze_campaign_ids in braze_api_campaign_ids.items() -%}
  {%- set outer_loop = loop -%}
  {%- for braze_campaign_id in braze_campaign_ids %}
  ('{{ brand_code }}', '{{ campaign_type }}', '{{ braze_campaign_id }}'){{ "," if not (outer_loop.last and loop.last) }}
  {% endfor %}
{%- endfor %}
;
