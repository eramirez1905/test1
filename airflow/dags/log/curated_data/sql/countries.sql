-- GEO_API has been replaced by the Porygon application.
-- In the data migration, discontinued countries and deleted records are not present in Porygon.
-- Here we need to merge GEO_API and Porygon data to have all historical records.

CREATE TEMP FUNCTION parse_tags_json(json STRING)
RETURNS STRUCT<delivery_types ARRAY<STRING>,
  cuisine ARRAY<STRING>,
  halal BOOL,
  tag ARRAY<STRING>,
  vertical_type STRING,
  characteristic ARRAY<STRING>
  >
LANGUAGE js AS """
  return JSON.parse(json);
""";

CREATE TEMP FUNCTION parse_filters(json STRING)
RETURNS ARRAY<
  STRUCT<
    key STRING,
    conditions ARRAY<
      STRUCT<
        operator STRING,
        values ARRAY<STRING>
      >
    >
  >
>
LANGUAGE js AS """
  const filtersRaw = JSON.parse(json) || [];
  const final = [];
  filtersRaw.forEach(elements => {
    Object.keys(elements)
      .forEach((key) => {
        const conditions = [];
        Object.keys(elements[key])
          .forEach((operator) => {
            let values = elements[key][operator];
            if (!Array.isArray(values)) {
              values = [values];
            }
            conditions.push({operator, values})
          })
        final.push({key, conditions})
      });
  });
  return final;
"""
;
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.countries` AS
WITH dps_entity_data AS (
  SELECT DISTINCT global_entity_id AS entity_id
  FROM `{{ params.project_id }}.dl.dynamic_pricing_vendor`
), travel_time_data AS (
  SELECT global_entity_id AS entity_id
    , ARRAY_AGG(
        STRUCT(formula AS travel_time_formula
          , CAST(regexp_replace(formula, '[^0-9.]', '') AS FLOAT64) AS travel_time_multiplier
          , created_at
          , updated_at
      )) AS travel_time
  FROM `{{ params.project_id }}.dl.dynamic_pricing_travel_time_formula`
  GROUP BY 1
), delivery_fee_range_data AS (
  SELECT global_entity_id AS entity_id
    , ARRAY_AGG(
        STRUCT(min_delivery_fee
          , max_delivery_fee
          , created_at
          , updated_at
      )) AS delivery_fee_range
  FROM `{{ params.project_id }}.dl.dynamic_pricing_delivery_fee_range`
  GROUP BY 1
), dps_config_data AS (
  SELECT entity_id
    , ARRAY_AGG(
        STRUCT(travel_time
          , delivery_fee_range
      )) dps_config
  FROM dps_entity_data
  LEFT JOIN travel_time_data USING (entity_id)
  LEFT JOIN delivery_fee_range_data USING (entity_id)
  GROUP BY 1
), entities AS (
  SELECT e.region
    , e.country_iso
    , country_code
    , e.country_name
    -- prefer data_fridge_global_entity_ids currency_code, however fall back to entities for the old entities not in data_fridge.
    , COALESCE(d.currency_iso_code, e.currency_code) AS currency_code
    , e.region_short_name
    , ARRAY_AGG(STRUCT(p.display_name
        , COALESCE(p.is_active, FALSE) AS is_active
        , p.entity_id
        , p.brand_id
        , p.hurrier_platforms
        , p.rps_platforms
        , p.brand_name
        , dc.dps_config
      ) ORDER BY display_name, entity_id) AS platforms
  FROM `{{ params.project_id }}.cl.entities` e
  LEFT JOIN UNNEST(platforms) p
  LEFT JOIN UNNEST(p.country_codes) country_code
  LEFT JOIN `{{ params.project_id }}.dl.data_fridge_global_entity_ids` d ON d.global_entity_id = p.entity_id
  LEFT JOIN dps_config_data dc USING (entity_id)
  GROUP BY 1, 2, 3, 4, 5, 6
), goal_delivery_time_zones_cleaned AS (
  SELECT country_code
    , external_id AS zone_id
    , created_at
    , goal_delivery_time
    -- Table contains updated_at that do not update any other columns, thus they cause noise. For this reason we are getting the latest updated_at only.
    , MAX(updated_at) AS updated_at
  FROM `{{ params.project_id }}.hl.forecast_zones`
  GROUP BY 1, 2, 3, 4
), goal_delivery_time_zones AS (
  SELECT zone_id
    , country_code
    , ARRAY_AGG(STRUCT(goal_delivery_time
        , updated_at
      ) ORDER BY updated_at DESC) AS goal_delivery_time_history
  FROM goal_delivery_time_zones_cleaned
  GROUP BY 1, 2
), goal_delivery_time_cities_cleaned AS (
  SELECT country_code
    , external_id AS city_id
    , created_at
    , goal_delivery_time
    -- Table contains updated_at that do not update any other columns, thus they cause noise. For this reason we are getting the latest updated_at only.
    , MAX(updated_at) AS updated_at
  FROM `{{ params.project_id }}.hl.forecast_cities`
  GROUP BY 1, 2, 3, 4
), goal_delivery_time_cities AS (
  SELECT city_id
    , country_code
    , ARRAY_AGG(STRUCT(goal_delivery_time
        , updated_at
      ) ORDER BY updated_at DESC) AS goal_delivery_time_history
  FROM goal_delivery_time_cities_cleaned
  GROUP BY 1, 2
), starting_points_dataset AS (
  SELECT sp.* EXCEPT(_row_number)
    , fsp.demand_distribution
  FROM (
    SELECT *
      , ROW_NUMBER() OVER (PARTITION BY country_code, id ORDER BY updated_at DESC) AS _row_number
    FROM (
      SELECT 'geo_api' AS source_app
        , sp.country_code
        , sp.id
        , sp.zone_id
        , sp.name
        , COALESCE(sp.active, FALSE) AS is_active
        , sp.created_at
        , sp.updated_at
        , SAFE.ST_GEOGFROMTEXT(sp.geo_wkt) AS shape
        , sp.geo_geojson AS geojson
        , sp.geo_wkt AS wkt
      FROM `{{ params.project_id }}.dl.geo_api_starting_point` sp
      UNION ALL
      SELECT 'porygon' AS source_app
        , sp.country_code
        , sp.id
        , sp.zone_id
        , sp.name
        , COALESCE(sp.is_active, FALSE) AS is_active
        , sp.created_at
        , sp.updated_at
        , SAFE.ST_GEOGFROMTEXT(sp.shape_wkt) AS shape
        , sp.shape_geojson AS geojson
        , sp.shape_wkt AS wkt
      FROM `{{ params.project_id }}.dl.porygon_starting_points` AS sp
    )
  ) sp
  LEFT JOIN `{{ params.project_id }}.dl.forecast_starting_points` fsp ON fsp.country_code = sp.country_code
    AND fsp.external_id = sp.id
  WHERE _row_number = 1
), starting_points AS (
  SELECT sp.country_code
    , sp.zone_id
    , ARRAY_AGG(STRUCT(
        sp.id
      , sp.zone_id
      , sp.name
      , COALESCE(sp.is_active, FALSE) AS is_active
      , sp.created_at
      , sp.updated_at
      , sp.shape
      , sp.demand_distribution
      , STRUCT(sp.geojson
          , sp.wkt
        ) AS starting_point_shape
    ) ORDER BY sp.id) AS starting_points
  FROM starting_points_dataset AS sp
  GROUP BY 1,2
), zones_dataset AS (
  SELECT * EXCEPT(_row_number)
  FROM (
    SELECT *
      , ROUND(ST_AREA(shape) / 1000 / 1000, 2) AS area
      , CASE
          WHEN ends_with(geo_id, '_halal') THEN ['halal']
          WHEN ends_with(geo_id, '_lcbo') THEN ['alcohol']
          ELSE []
        END AS delivery_types
      , ARRAY_CONCAT(
          IF(ends_with(geo_id, '_walker'), ['walker'], [])
        ) AS vehicle_types
      , ROW_NUMBER() OVER (PARTITION BY country_code, id ORDER BY updated_at DESC) AS _row_number
    FROM (
      SELECT 'geo_api' AS source_app
        , z.country_code
        , z.city_id
        , z.id
        , z.name
        , z.geo_id
        , active AS is_active
        , z.geo_updated_at AS zone_shape_updated_at
        , z.created_at
        , z.updated_at
        , SAFE.ST_GEOGFROMTEXT(z.geo_wkt) AS shape
        , z.geo_geojson AS geojson
        , z.geo_wkt AS wkt
      FROM `{{ params.project_id }}.dl.geo_api_zone` AS z
      UNION ALL
      SELECT 'porygon' AS source_app
        , z.country_code
        , z.city_id
        , z.id
        , z.name
        -- TODO: fix nulls in geo_id until its fixed on porygon side
        , COALESCE(z.geo_id, LOWER(REPLACE(z.name, ' ', '_'))) AS geo_id
        , COALESCE(z.is_active, FALSE) AS is_active
        , z.shape_updated_at AS zone_shape_updated_at
        , z.created_at
        , z.updated_at
        , SAFE.ST_GEOGFROMTEXT(z.shape_wkt) AS shape
        , z.shape_geojson AS geojson
        , z.shape_wkt AS wkt
      FROM `{{ params.project_id }}.dl.porygon_zones` AS z
    )
  )
  WHERE _row_number = 1
), zones_embedded AS (
  SELECT z1.country_code
    , z1.id
    , ARRAY_AGG(z2.id IGNORE NULLS ORDER BY z2.id) AS embedding_zone_ids
    , COUNT(z2.id) > 0 AS is_embedded
  FROM zones_dataset z1
  LEFT JOIN zones_dataset z2 ON z1.country_code = z2.country_code
    AND z2.is_active IS TRUE
    AND z1.city_id = z2.city_id
    AND z1.id != z2.id
    -- instead of ST_COVEREDBY use area of difference with some tolerance (5%)
    -- to capture almost fully embedded zones
    AND ST_INTERSECTS(z2.shape, z1.shape)
    AND (ST_AREA(ST_DIFFERENCE(z1.shape, z2.shape)) / 1000 / 1000) / NULLIF(z1.area, 0) < 0.05
  GROUP BY 1,2
), default_deliveryarea_settings AS (
  SELECT a.country_code
    , a.zone_id
    , SAFE_CAST(COALESCE(JSON_EXTRACT_SCALAR(b.backend_settings, '$.fee'), JSON_EXTRACT_SCALAR(b.backend_settings, '$.delivery_fee')) AS FLOAT64) AS delivery_fee
    , SAFE_CAST(JSON_EXTRACT_SCALAR(b.backend_settings, '$.feelsPercentage') AS BOOL) AS feels_percentage
    , SAFE_CAST(JSON_EXTRACT_SCALAR(b.backend_settings, '$.type') AS STRING) AS delivery_fee_type
    , SAFE_CAST(JSON_EXTRACT_SCALAR(b.backend_settings, '$.delivery_time') AS FLOAT64) AS delivery_time
    , SAFE_CAST(JSON_EXTRACT_SCALAR(b.backend_settings, '$.municipality_tax') AS FLOAT64) AS municipality_tax
    , SAFE_CAST(JSON_EXTRACT_SCALAR(b.backend_settings, '$.municipality_tax_type') AS STRING) AS municipality_tax_type
    , SAFE_CAST(JSON_EXTRACT_SCALAR(b.backend_settings, '$.tourist_tax') AS FLOAT64) AS tourist_tax
    , SAFE_CAST(JSON_EXTRACT_SCALAR(b.backend_settings, '$.tourist_tax_type') AS STRING) AS tourist_tax_type
    , SAFE_CAST(JSON_EXTRACT_SCALAR(b.backend_settings, '$.status') AS STRING) AS status
    , SAFE_CAST(COALESCE(JSON_EXTRACT_SCALAR(b.backend_settings, '$.minimum_value'), JSON_EXTRACT_SCALAR(b.backend_settings, '$.minimum_order_value')) AS FLOAT64) AS minimum_value
    , b.drive_time
    , a.vehicle_profile
    , a.cut_on_zone_border
    , a.priority
    , parse_filters(a.filters) AS filters
  FROM `{{ params.project_id }}.dl.porygon_default_deliveryarea_settings` a
  LEFT JOIN `{{ params.project_id }}.dl.porygon_default_deliveryareas` b ON a.country_code = b.country_code
    AND a.id = b.setting_id
), deliveryarea_settings AS (
  SELECT da.country_code
    , da.zone_id
    , ARRAY_AGG(STRUCT(
        STRUCT(
            IF(COALESCE(da.delivery_fee_type, IF(da.feels_percentage IS TRUE, 'percentage', 'amount')) = 'amount', da.delivery_fee, NULL) AS amount
          , IF(COALESCE(da.delivery_fee_type, IF(da.feels_percentage IS TRUE, 'percentage', 'amount')) = 'percentage', da.delivery_fee, NULL) AS percentage
        ) AS delivery_fee
        , da.delivery_time
        , da.municipality_tax
        , da.municipality_tax_type
        , da.tourist_tax
        , da.tourist_tax_type
        , da.status
        , da.minimum_value
        , da.drive_time
        , da.vehicle_profile
        , da.cut_on_zone_border
        , da.priority
        , da.filters
      )
    ) AS delivery_area_settings
  FROM default_deliveryarea_settings da
  GROUP BY 1, 2
), porygon_events AS (
  SELECT * EXCEPT(tags)
    , parse_tags_json(tags) AS tags
  FROM `{{ params.project_id }}.dl.porygon_events`
), events AS (
  SELECT pe.country_code
    , pe.zone_id
    , ARRAY_AGG(STRUCT(pe.id
        , pe.action
        , pe.activation_threshold
        , pe.deactivation_threshold
        , pe.starts_at
        , pe.ends_at
        , pe.title
        , pe.value
        , pe.message
        , pe.updated_at
        , pe.shape_sync AS is_shape_in_sync
        , SAFE.ST_GEOGFROMTEXT(pe.shape_wkt) AS shape
        , tags.delivery_types AS delivery_provider
        , tags.cuisine AS cuisines
        , tags.halal AS is_halal
        , tags.tag AS tags
        , tags.vertical_type AS vertical_type
        , tags.characteristic AS characteristics
      ) ORDER BY pe.id
    ) AS events
  FROM porygon_events AS pe
  GROUP BY 1,2
), zone_opening_times AS (
  SELECT country_code
    , zone_external_id AS zone_id
    , ARRAY_AGG(STRUCT(weekday
        ,`from` AS starts_at
        , `to` AS ends_at
    )) AS opening_times
  FROM `{{ params.project_id }}.dl.forecast_opening_times`
  GROUP BY 1, 2
  ORDER BY 1, 2
), zones AS (
  SELECT z.country_code
    , z.city_id
    , ARRAY_AGG(STRUCT(z.id
      , z.city_id
      , z.name
      , geo_id
      , hz.fleet_id
      , COALESCE(z.is_active, FALSE) AS is_active
      , d.zone_id IS NOT NULL AS has_default_delivery_area_settings
      , d.delivery_area_settings AS default_delivery_area_settings
      , z.zone_shape_updated_at
      , z.created_at
      , z.updated_at
      , shape
      , delivery_types
      , vehicle_types
      , CASE
          WHEN 'walker' IN UNNEST(vehicle_types)
            THEN 1000
          ELSE NULL
        END AS distance_cap
      , area
      , ST_BOUNDARY(z.shape) AS boundaries
      , ze.embedding_zone_ids
      , ze.is_embedded
      , STRUCT(z.geojson
          , z.wkt
        ) AS zone_shape
      , sp.starting_points
      , zs.events AS events
      , op.opening_times
      , g.goal_delivery_time_history
    ) ORDER BY z.name) AS zones
  FROM zones_dataset AS z
  LEFT JOIN zones_embedded ze ON ze.country_code = z.country_code
    AND ze.id = z.id
  LEFT JOIN starting_points sp ON sp.country_code = z.country_code
    AND sp.zone_id = z.id
  LEFT JOIN `{{ params.project_id }}.dl.hurrier_geo_zones` hz ON hz.country_code = z.country_code
    AND hz.id = z.id
  LEFT JOIN deliveryarea_settings d ON z.country_code = d.country_code
    AND d.zone_id = z.id
  LEFT JOIN events zs ON z.country_code = zs.country_code
    AND z.id = zs.zone_id
  LEFT JOIN zone_opening_times op ON z.country_code = op.country_code
   AND z.id = op.zone_id
  LEFT JOIN goal_delivery_time_zones g ON z.country_code = g.country_code
   AND z.id = g.zone_id
  GROUP BY 1,2
), cities_datatset AS (
  SELECT * EXCEPT(_row_number)
  FROM (
    SELECT *
      , ROW_NUMBER() OVER (PARTITION BY country_code, id ORDER BY updated_at DESC) AS _row_number
    FROM (
      SELECT 'geo_api' AS source_app
        , c.country_code
        , c.id
        , c.name
        , COALESCE(c.active, FALSE) AS is_active
        , c.timezone
        , c.created_at
        , c.updated_at
        , hc.order_value_limit
      FROM `{{ params.project_id }}.dl.geo_api_city` AS c
      LEFT JOIN `{{ params.project_id }}.dl.hurrier_cities` hc ON hc.country_code = c.country_code
        AND hc.id = c.id
      UNION ALL
      SELECT 'porygon' AS source_app
        , c.country_code
        , c.id
        , c.name
        , COALESCE(c.is_active, FALSE) AS is_active
        , c.timezone
        , c.created_at
        , c.updated_at
        , hc.order_value_limit
      FROM `{{ params.project_id }}.dl.porygon_cities` AS c
      LEFT JOIN `{{ params.project_id }}.dl.hurrier_cities` hc ON hc.country_code = c.country_code
        AND hc.id = c.id
    )
  )
  WHERE _row_number = 1
), cities AS (
  SELECT c.country_code
    , ARRAY_AGG(STRUCT(c.id
        , c.name
        , COALESCE(c.is_active, FALSE) AS is_active
        , c.timezone
        , c.created_at
        , c.updated_at
        , hc.order_value_limit
        , zones
        , g.goal_delivery_time_history
      ) ORDER BY c.name) AS cities
  FROM cities_datatset AS c
  LEFT JOIN `{{ params.project_id }}.dl.hurrier_cities` hc ON hc.country_code = c.country_code
    AND hc.id = c.id
  LEFT JOIN zones z ON z.country_code = c.country_code
    AND z.city_id = c.id
  LEFT JOIN goal_delivery_time_cities g ON g.country_code = c.country_code
    AND g.city_id = c.id
  GROUP BY 1
)
SELECT e.region
  , e.region_short_name
  -- By mistake we have DarkStore country_code with underscore instead of hyphen
  , IF(STARTS_WITH(ic.country_code, 'ds-'), REPLACE(ic.country_code, '-', '_'), ic.country_code) as country_code
  , e.country_iso
  , e.country_name
  , e.currency_code
  , e.platforms
  , c.cities
FROM `{{ params.project_id }}.dl.iam_countries` ic
-- By mistake we have DarkStore pickers with underscore instead of hyphen
FULL JOIN entities e ON IF(starts_with(e.country_code, 'ds_'), REPLACE(e.country_code, '_', '-'), e.country_code) = ic.country_code
LEFT JOIN cities c ON IF(starts_with(c.country_code, 'ds_'), REPLACE(c.country_code, '_', '-'), c.country_code) = ic.country_code
ORDER BY country_iso
