CREATE TEMP FUNCTION json2array(json STRING)
RETURNS ARRAY<STRING>
LANGUAGE js AS """
  return JSON.parse(json).map(x => JSON.stringify(x));
""";

CREATE TEMP FUNCTION parse_tags_json(json STRING)
RETURNS STRUCT<delivery_provider ARRAY<STRING>, delivery_types ARRAY<STRING>, cuisine ARRAY<STRING>, halal BOOL>
LANGUAGE js AS """
  return JSON.parse(json);
""";

CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.vendors` AS
WITH location_history_dataset AS (
  SELECT country_code
      , id
      , ARRAY(
            SELECT SAFE.ST_GEOGPOINT(
              CAST(JSON_EXTRACT_SCALAR(location, '$.longitude') AS FLOAT64),
              CAST(JSON_EXTRACT_SCALAR(location, '$.latitude') AS FLOAT64)
            )
            FROM UNNEST(locations) location
      ) AS location_history
  FROM (
    SELECT country_code
      , id
      , json2array(JSON_EXTRACT(location_history, '$')) as locations
    FROM `{{ params.project_id }}.dl.hurrier_businesses`
    WHERE location_history IS NOT NULL
  )
), orders AS (
  SELECT country_code
    , vendor_id
    , ARRAY_AGG(DISTINCT entity.id IGNORE NULLS ORDER BY entity.id) AS entity_ids
  FROM `{{ params.project_id }}.cl._orders`
  GROUP BY 1,2
), restaurants AS (
  SELECT * EXCEPT(tags)
    , parse_tags_json(tags) AS tags
  FROM (
    SELECT country_code
       , vendor_id
       , platform
       , tags
       , ROW_NUMBER() OVER (PARTITION BY country_code, COALESCE(vendor_id,  id) ORDER BY updated_at DESC) AS _row_number
      FROM `{{ params.project_id }}.dl.porygon_restaurants`
  )
  WHERE _row_number = 1
), delivery_areas AS (
  SELECT v.country_code
    , v.platform
    , v.id
    , ANY_VALUE(IF(d.id IS NULL, TRUE, FALSE)) AS is_deleted
  FROM `{{ params.project_id }}.dl.porygon_deliveryareas_versions` v
  LEFT JOIN `{{ params.project_id }}.dl.porygon_deliveryareas` d USING (country_code, platform, id)
  GROUP BY 1, 2, 3
), delivery_areas_settings AS (
  SELECT country_code
    , id
    , platform
    , transaction_id
    , SAFE_CAST(COALESCE(JSON_EXTRACT_SCALAR(v.backend_settings, '$.fee'), JSON_EXTRACT_SCALAR(v.backend_settings, '$.delivery_fee')) AS FLOAT64) AS delivery_fee
    , SAFE_CAST(JSON_EXTRACT_SCALAR(v.backend_settings, '$.feelsPercentage') AS BOOL) AS feels_percentage
    , SAFE_CAST(JSON_EXTRACT_SCALAR(v.backend_settings, '$.type') AS STRING) AS delivery_fee_type
    , SAFE_CAST(JSON_EXTRACT_SCALAR(v.backend_settings, '$.delivery_time') AS FLOAT64) AS delivery_time
    , SAFE_CAST(JSON_EXTRACT_SCALAR(v.backend_settings, '$.municipality_tax') AS FLOAT64) AS municipality_tax
    , SAFE_CAST(JSON_EXTRACT_SCALAR(v.backend_settings, '$.municipality_tax_type') AS STRING) AS municipality_tax_type
    , SAFE_CAST(JSON_EXTRACT_SCALAR(v.backend_settings, '$.tourist_tax') AS FLOAT64) AS tourist_tax
    , SAFE_CAST(JSON_EXTRACT_SCALAR(v.backend_settings, '$.tourist_tax_type') AS STRING) AS tourist_tax_type
    , SAFE_CAST(JSON_EXTRACT_SCALAR(v.backend_settings, '$.status') AS STRING) AS status
    , SAFE_CAST(COALESCE(JSON_EXTRACT_SCALAR(v.backend_settings, '$.minimum_value'), JSON_EXTRACT_SCALAR(v.backend_settings, '$.minimum_order_value')) AS FLOAT64) AS minimum_value
  FROM `{{ params.project_id }}.dl.porygon_deliveryareas_versions` v
), delivery_areas_history AS (
  SELECT da.country_code
    , v.restaurant_id AS vendor_code
    , da.id
    , da.platform
    , da.is_deleted
    , ARRAY_AGG(STRUCT(v.id
        , v.created_date
        -- will be deprecated on 12/29/2019
        , v.created_at
        -- will be deprecated on 12/29/2019
        , v.updated_at
        , v.created_at AS active_from
        , v.updated_at AS active_to
        , CASE
            WHEN operation_type = 0
              THEN 'created'
            WHEN operation_type = 1
              THEN 'updated'
            WHEN operation_type = 2
              THEN 'cancelled'
            ELSE NULL
          END AS operation_type
        , SAFE.ST_GEOGFROMTEXT(v.shape_wkt) AS shape
        , v.transaction_id
        , v.end_transaction_id
        , STRUCT(
            STRUCT(
                IF(COALESCE(s.delivery_fee_type, IF(s.feels_percentage IS TRUE, 'percentage', 'amount')) = 'amount', s.delivery_fee, NULL) AS amount
              , IF(COALESCE(s.delivery_fee_type, IF(s.feels_percentage IS TRUE, 'percentage', 'amount')) = 'percentage', s.delivery_fee, NULL) AS percentage
            ) AS delivery_fee
            , s.delivery_time
            , s.municipality_tax
            , s.municipality_tax_type
            , s.tourist_tax
            , s.tourist_tax_type
            , s.minimum_value
          ) AS settings
        , v.name
        , v.drive_time
    ) ORDER BY v.updated_at) AS history
  FROM delivery_areas da
  LEFT JOIN `{{ params.project_id }}.dl.porygon_deliveryareas_versions` v ON da.country_code = v.country_code
    AND da.platform = v.platform
    AND da.id = v.id
  LEFT JOIN delivery_areas_settings s ON v.country_code = s.country_code
    AND v.id = s.id
    AND v.platform = s.platform
    AND v.transaction_id = s.transaction_id
  GROUP BY 1, 2, 3, 4, 5
), delivery_areas_dataset AS (
  SELECT * EXCEPT(is_deleted)
    , COALESCE(is_deleted, (SELECT h.operation_type FROM UNNEST(history) h ORDER BY h.updated_at DESC, h.transaction_id DESC LIMIT 1) = 'cancelled') AS is_deleted
  FROM delivery_areas_history
), delivery_areas_final AS (
  SELECT country_code
    , vendor_code
    , ARRAY_AGG(STRUCT(id
        , platform
        , is_deleted
        , ARRAY(SELECT AS STRUCT * EXCEPT(id, created_date) FROM UNNEST(history)) AS history
    )) AS delivery_areas
  FROM delivery_areas_dataset
  GROUP BY  1, 2
), tes_restaurants_dataset AS (
  SELECT * EXCEPT(_row_number)
  FROM (
    SELECT country_code
      , external_id
      , vertical_type
      , ROW_NUMBER() OVER (PARTITION BY country_code, external_id ORDER BY created_at DESC) AS _row_number
    FROM `{{ params.project_id }}.dl.tes_restaurant`
    WHERE vertical_type IS NOT NULL
  )
  WHERE _row_number = 1
)
SELECT b.country_code
  , a.city_id
  , b.id AS vendor_id
  , b.unn_id AS platform_vendor_id
  , r.platform AS porygon_platform
  , b.unn_code AS vendor_code
  , b.name AS vendor_name
  , b.order_value_limit
  , b.global_entity_id
  , o.entity_ids
  , b.arriving_time AS walk_in_time
  , b.leaving_time AS walk_out_time
  , a.geo_point AS location
  , SAFE.ST_GEOGPOINT(
      CAST(JSON_EXTRACT_SCALAR(b.last_provided_location, '$.last_longitude') AS FLOAT64),
      CAST(JSON_EXTRACT_SCALAR(b.last_provided_location, '$.last_latitude') AS FLOAT64)
    ) AS last_provided_location
  , COALESCE(h.location_history, []) AS location_history
  , ARRAY(SELECT DISTINCT x FROM UNNEST(ARRAY_CONCAT(COALESCE(tags.delivery_provider, []), COALESCE(tags.delivery_types, []))) x) AS delivery_provider
  , tags.cuisine AS cuisines
  , tags.halal AS is_halal
  , b.created_at
  , b.updated_at
  , d.delivery_areas
  , tr.vertical_type
FROM `{{ params.project_id }}.dl.hurrier_businesses` b
LEFT JOIN location_history_dataset h ON h.country_code = b.country_code
  AND h.id = b.id
LEFT JOIN `{{ params.project_id }}.cl._addresses` a ON a.country_code = b.country_code
  AND b.address_id = a.id
LEFT JOIN restaurants r ON b.country_code = r.country_code
  AND b.unn_code = r.vendor_id
LEFT JOIN delivery_areas_final d ON d.country_code = b.country_code
  AND d.vendor_code = b.unn_code
LEFT JOIN orders o ON o.country_code = b.country_code
  AND o.vendor_id = b.id
LEFT JOIN tes_restaurants_dataset tr ON b.country_code = tr.country_code
    AND b.unn_code = tr.external_id
