CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.acl_dataset }}.users` AS
WITH users AS (
{%- for venture in params.ventures %}
    -- start venture='{{ venture.name }}' block
    SELECT '{{ venture.name }}' AS venture_name,
      {% if venture.domain -%}
      '{{ venture.domain }}' AS `email`,
      {% else -%}
      NULL AS `email`,
      {% endif -%}
      0 AS weight,
      {%- if venture.domain is none %}
        [] AS countries,
        [] AS entities
      {%- elif venture.countries is none %}
        ARRAY((SELECT DISTINCT country_code FROM `{{ params.project_id }}.{{ params.cl_dataset }}.countries` WHERE country_code IS NOT NULL)) AS countries,
        ARRAY((SELECT DISTINCT entity_id FROM `{{ params.project_id }}.{{ params.cl_dataset }}.entities` LEFT JOIN UNNEST(platforms) WHERE entity_id IS NOT NULL)) AS entities
      {%- else %}
        {{ venture.countries }} AS countries,
        {{ venture.entities }} AS entities
      {%- endif %}
    {%- for user in venture.users if user.email %}
      UNION ALL
      SELECT '{{ venture.name }}' AS venture_name,
        '{{ user.email }}' AS `email`,
        1 AS weight,
        {%- if venture.countries is none %}
          ARRAY((SELECT DISTINCT country_code FROM `{{ params.project_id }}.{{ params.cl_dataset }}.countries` WHERE country_code IS NOT NULL)) AS countries,
          ARRAY((SELECT DISTINCT entity_id FROM `{{ params.project_id }}.{{ params.cl_dataset }}.entities` LEFT JOIN UNNEST(platforms) WHERE entity_id IS NOT NULL)) AS entities
        {%- else %}
          {{ venture.countries }} AS countries,
          {{ venture.entities }} AS entities
        {%- endif %}
    {%- endfor %}
    -- end venture='{{ venture.name }}' block
    {% if not loop.last -%}UNION ALL{% endif %}
{%- endfor %}
)
SELECT * FROM users
