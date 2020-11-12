CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._vendors_delivery_areas` AS
WITH delivery_areas_settings AS (
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
), audit_logs_raw AS (
  SELECT country_code
    , user_id
    , email
    , COALESCE(delivery_area.delivery_area_create.vendor_code, delivery_area.delivery_area_modify_or_create.vendor_code, delivery_area.delivery_area_delete.vendor_code) AS vendor_code
    , COALESCE(delivery_area.delivery_area_create.created_at, delivery_area.delivery_area_modify_or_create.created_at, delivery_area.delivery_area_delete.created_at) AS created_at
    , COALESCE(delivery_area.delivery_area_create.updated_at, delivery_area.delivery_area_modify_or_create.updated_at, delivery_area.delivery_area_delete.updated_at) AS updated_at
    , COALESCE(delivery_area.delivery_area_create.id, delivery_area.delivery_area_modify_or_create.id, delivery_area.delivery_area_delete.id) AS id
    , COALESCE(delivery_area.delivery_area_create.operation_type, delivery_area.delivery_area_modify_or_create.operation_type, delivery_area.delivery_area_delete.operation_type) AS operation_type
  FROM `{{ params.project_id }}.cl.audit_logs`
  WHERE application = 'deliveryareas'
    AND action IN ('delivery_area_create', 'delivery_area_modify_or_create', 'delivery_area_delete')
), audit_logs_transformed AS (
  SELECT * EXCEPT(operation_type)
    , CASE
        WHEN operation_type = 'created'
          THEN 0
        WHEN operation_type = 'updated'
          THEN 1
        WHEN operation_type = 'deleted'
          THEN 2
        ELSE NULL
      END AS operation_type
  FROM audit_logs_raw
), audit_logs_delete AS (
  SELECT *
    , ROW_NUMBER() OVER (PARTITION BY country_code, id, vendor_code, operation_type ORDER BY updated_at DESC) AS _row_number
  FROM audit_logs_transformed
  WHERE operation_type = 2
), audit_logs_create_update AS (
  SELECT *
    , ROW_NUMBER() OVER (PARTITION BY country_code, id, vendor_code, operation_type, created_at, updated_at ORDER BY updated_at DESC) AS _row_number
  FROM audit_logs_transformed
  WHERE operation_type != 2
), audit_logs AS (
  SELECT * EXCEPT(_row_number)
  FROM audit_logs_create_update
  WHERE _row_number = 1
  UNION ALL
  SELECT * EXCEPT(_row_number)
  FROM audit_logs_delete
  WHERE _row_number = 1
), audit_logs_deliveryareas AS (
  SELECT v.country_code
    , v.id
    , v.platform
    , v.transaction_id
    , v.restaurant_id
    , a.email
    , a.user_id
  FROM `{{ params.project_id }}.dl.porygon_deliveryareas_versions` v
  LEFT JOIN audit_logs a ON v.restaurant_id = a.vendor_code
    AND v.id = a.id
    AND v.country_code = a.country_code
    AND v.operation_type = a.operation_type
    -- the following conditions are required to join tables based on operation_type except delete.
    AND CASE
          WHEN v.operation_type = 0
            THEN v.created_at = a.created_at
          WHEN v.operation_type = 1
            THEN v.created_at = a.updated_at
          ELSE TRUE
        END
  WHERE NOT (v.operation_type = 2 AND v.end_transaction_id IS NULL)
), delivery_areas_history AS (
  SELECT v.country_code
    , v.restaurant_id AS vendor_code
    , v.platform
    , v.id
    , ARRAY_AGG(STRUCT(
          v.created_at AS active_from
        , IF(end_transaction_id IS NULL AND v.operation_type != 2, NULL, v.updated_at) AS active_to
        , CASE
            WHEN v.operation_type = 0
              THEN 'created'
            WHEN v.operation_type = 1
              THEN 'updated'
            WHEN v.operation_type = 2
              THEN 'deleted'
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
            , s.status
            , s.minimum_value
          ) AS settings
        , v.name
        , STRUCT(a.user_id
            , a.email
          ) AS edited_by
        , v.drive_time
    ) ORDER BY v.created_at, v.updated_at) AS history
  FROM `{{ params.project_id }}.dl.porygon_deliveryareas_versions` v
  LEFT JOIN delivery_areas_settings s ON v.country_code = s.country_code
    AND v.id = s.id
    AND v.platform = s.platform
    AND v.transaction_id = s.transaction_id
  LEFT JOIN audit_logs_deliveryareas a ON v.country_code = a.country_code
    AND v.id = a.id
    AND v.platform = a.platform
    AND v.transaction_id = a.transaction_id
    AND v.restaurant_id = a.restaurant_id
  GROUP BY 1, 2, 3, 4
)
SELECT country_code
  , vendor_code
  , platform
  , ARRAY_AGG(STRUCT(id
      , country_code
      , platform
      , (SELECT h.operation_type FROM UNNEST(history) h ORDER BY h.active_to DESC, h.transaction_id DESC LIMIT 1) = 'deleted' AS is_deleted
      , history
    )) AS delivery_areas
FROM delivery_areas_history
GROUP BY 1, 2, 3
