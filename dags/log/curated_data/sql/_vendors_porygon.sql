CREATE TEMP FUNCTION parse_tags_json(json STRING)
RETURNS STRUCT<delivery_provider ARRAY<STRING>,
  delivery_types ARRAY<STRING>,
  cuisine ARRAY<STRING>,
  halal BOOL,
  tag ARRAY<STRING>,
  vertical_type STRING,
  customer_types ARRAY<STRING>,
  characteristic ARRAY<STRING>
  >
LANGUAGE js AS """
  return JSON.parse(json);
""";

CREATE TEMP FUNCTION fix_mapping(country_code STRING, platform STRING)
RETURNS STRING
AS (
  CASE
    WHEN country_code = 'ba' AND platform = 'efood' THEN 'DN_BA'
    WHEN country_code = 'hr' AND platform = 'efood' THEN 'PZ_HR'
    WHEN country_code = 'rs' AND platform = 'efood' THEN 'DN_RS'
    WHEN country_code = 'gr' AND platform = 'efood' THEN 'EF_RS'
    ELSE NULL
  END
);

CREATE TEMP FUNCTION parse_chain_json(json STRING)
RETURNS STRUCT<id STRING,
  name STRING
  >
LANGUAGE js AS """
  const chain = JSON.parse(json);
  let id, name;
  if (chain) {
    id = Object.keys(chain)[0];
    name = chain[id];
  }
  return {id, name};
""";

CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._vendors_porygon` AS
WITH countries AS (
  SELECT country_code, country_iso
  FROM `{{ params.project_id }}.cl.countries` c
  WHERE country_code IS NOT NULL
  GROUP BY 1,2
), pandora_entites AS (
  SELECT country_code, country_iso, p.entity_id
  FROM `{{ params.project_id }}.cl.countries` c
  LEFT JOIN UNNEST(c.platforms) p
  WHERE country_code IS NOT NULL
    AND (STARTS_WITH(p.entity_id, 'FO_') OR STARTS_WITH(p.entity_id, 'FP_'))
), delivery_areas AS (
  SELECT *
  FROM `{{ params.project_id }}.cl._vendors_delivery_areas`
), porygon_restaurants AS (
  SELECT v.* EXCEPT(tags, chain)
    , v.id AS vendor_code
    , parse_tags_json(v.tags) AS tags
    , parse_chain_json(v.chain) AS chain
    , c.country_iso
    , da.delivery_areas
  FROM `{{ params.project_id }}.dl.porygon_restaurants` v
  LEFT JOIN countries c ON c.country_code = v.country_code
  LEFT JOIN delivery_areas da ON da.country_code = v.country_code
    AND da.platform = v.platform
    AND da.vendor_code = v.id
), porygon_active_history_calculation AS (
  SELECT id
    , platform
    , country_code
    , is_active
    , updated_at
    , LAG(is_active) OVER (PARTITION BY id, platform, country_code ORDER BY updated_at ASC) AS previous_status
  FROM `{{ params.project_id }}.hl.porygon_restaurants`
), porygon_active_history AS (
  SELECT id
    , platform
    , country_code
    , ARRAY_AGG(
        STRUCT(is_active
          , updated_at
        )
        ORDER BY updated_at DESC
    ) AS is_active_history
  FROM porygon_active_history_calculation
  WHERE is_active <> previous_status OR previous_status IS NULL
  GROUP BY 1,2,3
),  porygon_drive_time_data AS (
  SELECT country_code
  , platform
  , vendor_code
  , polygon_drive_times
  FROM `{{ params.project_id }}.cl._porygon_drive_time_polygons`
), porygon_entity_mapping AS (
  SELECT v.country_code
    , v.country_iso
    , v.platform
    , v.id
    , COALESCE(global_entity_id,
        CASE
          WHEN v.platform = 'carriage' THEN CONCAT('CG', '_', v.country_iso)
          WHEN v.platform = 'talabat' THEN CONCAT('TB', '_', v.country_iso)
          WHEN v.platform = 'pedidosya' THEN CONCAT('PY', '_', v.country_iso)
          WHEN v.platform = 'apetito24' AND country_code = 'pa' THEN CONCAT('PY', '_', v.country_iso)
          WHEN v.platform = 'mjam' THEN CONCAT('MJM', '_', v.country_iso)
          WHEN v.platform = 'boozer' THEN CONCAT('BO', '_', v.country_iso)
          WHEN v.platform = 'domicilios' THEN CONCAT('CD', '_', v.country_iso)
          WHEN v.platform = 'damejidlo' THEN CONCAT('DJ', '_', v.country_iso)
          WHEN v.platform = 'domicilios' THEN CONCAT('CD', '_', v.country_iso)
          WHEN v.platform = 'pizzaonline' THEN CONCAT('PO', '_', v.country_iso)
          WHEN v.platform = 'efood' THEN fix_mapping(v.country_code, v.platform)
          WHEN v.platform = 'donesi' THEN CONCAT('DN', '_', v.country_iso)
          WHEN v.platform = 'netpincer' THEN CONCAT('NP', '_', v.country_iso)
          WHEN v.platform = 'hungerstation' THEN CONCAT('HS', '_', v.country_iso)
          WHEN v.platform = 'onlinepizza' THEN CONCAT('OP', '_', v.country_iso)
          WHEN v.platform = 'yemeksepeti' THEN CONCAT('YS', '_', v.country_iso)
          WHEN v.platform = 'dhg' THEN CONCAT('LH', '_', v.country_iso)
          WHEN v.platform = 'foodpanda' AND v.country_code = 'de' THEN CONCAT('FO', '_', v.country_iso)
          WHEN v.platform = 'pandora' THEN (SELECT entity_id FROM pandora_entites pe WHERE pe.country_code = v.country_code)
          ELSE NULL
        END
      ) AS entity_id
  FROM porygon_restaurants v
), vendors AS (
  SELECT v.country_code
    , pe.entity_id
    , v.vendor_code
    , v.country_iso
    , v.platform
    , v.global_entity_id
    , name
    , created_at
    , updated_at
    , a.is_active_history
    , is_active
    , vehicle_profile
    , ARRAY(
        SELECT DISTINCT
          CASE
            WHEN delivery_types IN ('Hurrier', 'platform_delivery')
              THEN 'OWN_DELIVERY'
            WHEN delivery_types IN ('vendor_delivery', 'partner_delivery', 'Restaurant')
              THEN 'VENDOR_DELIVERY'
            WHEN delivery_types = 'pickup'
              THEN 'PICKUP'
            ELSE
              NULL
          END
        FROM UNNEST(ARRAY_CONCAT(COALESCE(tags.delivery_provider, []), COALESCE(tags.delivery_types, []))) delivery_types
      ) AS delivery_provider
    , v.chain
    , tags.cuisine AS cuisines
    , tags.halal AS is_halal
    , tags.tag AS tags
    , tags.vertical_type AS vertical_type
    , tags.customer_types AS customer_types
    , tags.characteristic AS characteristics
    , SAFE.ST_GEOGFROMTEXT(location_wkt) AS location
    , delivery_areas
    , ROW_NUMBER() OVER (PARTITION BY pe.entity_id, v.vendor_code ORDER BY v.updated_at DESC) AS _row_number
    , pdt.polygon_drive_times
  FROM porygon_restaurants v
  LEFT JOIN porygon_entity_mapping pe ON pe.country_code = v.country_code
    AND pe.platform = v.platform
    AND pe.id = v.id
  LEFT JOIN porygon_active_history a ON a.country_code = v.country_code
    AND a.platform = v.platform
    AND a.id = v.id
  LEFT JOIN porygon_drive_time_data pdt ON pdt.country_code = v.country_code
    AND pdt.vendor_code = v.vendor_code
    AND pdt.platform = v.platform
), vendors_agg AS (
  SELECT entity_id
    , vendor_code
    , ARRAY_AGG(STRUCT(
          v.country_code
        , v.platform
        , v.global_entity_id
        , v.name
        , v.location
        , v.created_at
        , v.updated_at
        , v.is_active
        , v.delivery_provider
        , v.vehicle_profile
        , v.cuisines
        , v.is_halal
        , v.tags
        , v.vertical_type
        , v.customer_types
        , v.characteristics
        , v.polygon_drive_times
      )) AS vendors
    , ARRAY_CONCAT_AGG(v.delivery_areas) AS delivery_areas
  FROM vendors v
  GROUP BY 1,2
)
SELECT v.entity_id
  , v.vendor_code
  , v.name
  , v.created_at
  , v.updated_at
  , v.is_active
  , v.is_active_history
  , v.delivery_provider
  , v.vehicle_profile
  , v.cuisines
  , v.is_halal
  , v.chain
  , v.tags
  , v.vertical_type
  , v.customer_types
  , v.characteristics
  , v.location
  , a.delivery_areas
  , a.vendors AS porygon
  , v.polygon_drive_times
FROM vendors v
LEFT JOIN vendors_agg a USING(entity_id, vendor_code)
WHERE _row_number = 1
