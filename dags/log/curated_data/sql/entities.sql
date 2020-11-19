CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.entities` AS
WITH entities AS (
  {%- for entity in params.entities %}
  {%- if not loop.first %}
  UNION ALL
  {%- endif %}
  SELECT '{{ entity.country_iso }}' AS country_iso, ARRAY<STRING>{{ entity.country_code | tojson }} AS country_codes, '{{ entity.common_name }}' AS common_name, '{{ entity.display_name }}' AS display_name, '{{ entity.currency_code }}' AS currency_code, {{ entity.is_active }} AS is_active, '{{ entity.entity_id }}' AS entity_id, '{{ entity.region }}' AS region
    , '{{ entity.entity_id.split('_', 1)[0] }}' AS brand
    , {{ entity.hurrier_platforms | default([]) }} AS hurrier_platforms
    , {{ entity.rps_platforms | default([]) }} AS rps_platforms
  {%- endfor %}
)
SELECT e.region
  , CASE
      WHEN region = 'Europe'
        THEN 'eu'
      WHEN country_iso = 'KR'
        THEN 'kr'
      WHEN region = 'Asia' OR region = 'Australia'
        THEN 'ap'
      WHEN region = 'Americas'
        THEN 'us'
      WHEN region = 'MENA'
        THEN 'mena'
        ELSE NULL
    END AS region_short_name
  , e.country_iso
  , e.common_name AS country_name
  , e.currency_code
  , ARRAY_AGG(STRUCT(e.display_name
      , e.country_codes
      , COALESCE(e.is_active, FALSE) AS is_active
      , e.entity_id
      , df.timezone
      , e.hurrier_platforms
      , e.rps_platforms
      , SPLIT(e.entity_id, '_')[OFFSET(0)] AS brand_id
      , df.brand_name
    ) ORDER BY display_name, entity_id) AS platforms
FROM entities e
LEFT JOIN `{{ params.project_id }}.dl.data_fridge_global_entity_ids` df ON e.entity_id = df.global_entity_id
GROUP BY 1,2,3,4,5
ORDER BY country_iso
