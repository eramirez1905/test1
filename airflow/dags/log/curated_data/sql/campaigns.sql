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
  const filters = [];
  const filtersRaw = JSON.parse(json) || [];
  function ParseObj(elements) {
    Object.keys(elements)
          .forEach((key) => {
            const conditions = [];
            Object.keys(elements[key])
              .forEach((operator) => {
                let values = elements[key][operator] || [];
                if (!Array.isArray(values)) {
                  values = [values];
                }
                conditions.push({operator, values})
              });
            filters.push({key, conditions})
          });
  }
  if (Array.isArray(filtersRaw)) {
    filtersRaw.forEach(elements => {
      ParseObj(elements);
    });
  }
  else {
    ParseObj(filtersRaw);
  }
  return filters;
"""
;
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.campaigns` AS
WITH campaigns AS (
  SELECT country_code
    , id AS campaign_id
  FROM `{{ params.project_id }}.dl.porygon_campaigns_versions`
  GROUP BY 1, 2
), campaigns_settings AS (
   SELECT country_code
    , id
    , platform
    , transaction_id
    , SAFE_CAST(COALESCE(JSON_EXTRACT_SCALAR(v.backend_settings, '$.fee'), JSON_EXTRACT_SCALAR(v.backend_settings, '$.delivery_fee')) AS FLOAT64) AS delivery_fee
    , SAFE_CAST(JSON_EXTRACT_SCALAR(v.backend_settings, '$.type') AS STRING) AS delivery_fee_type
    , SAFE_CAST(JSON_EXTRACT_SCALAR(v.backend_settings, '$.delivery_time') AS FLOAT64) AS delivery_time
    , SAFE_CAST(JSON_EXTRACT_SCALAR(v.backend_settings, '$.municipality_tax') AS FLOAT64) AS municipality_tax
    , SAFE_CAST(JSON_EXTRACT_SCALAR(v.backend_settings, '$.municipality_tax_type') AS STRING) AS municipality_tax_type
    , SAFE_CAST(JSON_EXTRACT_SCALAR(v.backend_settings, '$.status') AS STRING) AS status
    , SAFE_CAST(COALESCE(JSON_EXTRACT_SCALAR(v.backend_settings, '$.minimum_value'), JSON_EXTRACT_SCALAR(v.backend_settings, '$.minimum_order_value')) AS FLOAT64) AS minimum_value
    , parse_filters(v.filters) AS filters
  FROM `{{ params.project_id }}.dl.porygon_campaigns_versions` v
), campaigns_history AS (
  SELECT ca.country_code
    , v.platform
    , ca.campaign_id AS id
    , ARRAY_AGG(STRUCT(
          v.created_at AS active_from
        , IF(end_transaction_id IS NULL AND operation_type != 2, NULL, v.updated_at) AS active_to
        , CASE
            WHEN operation_type = 0
              THEN 'created'
            WHEN operation_type = 1
              THEN 'updated'
            WHEN operation_type = 2
              THEN 'deleted'
            ELSE NULL
          END AS operation_type
        , SAFE.ST_GEOGFROMTEXT(v.shape_wkt) AS shape
        , v.transaction_id
        , v.end_transaction_id
        , v.is_active
        , v.name
        , v.drive_times
        , STRUCT(
              STRUCT(
                  IF(cs.delivery_fee_type = 'amount', cs.delivery_fee, NULL) AS amount
                , IF(cs.delivery_fee_type = 'percentage', cs.delivery_fee, NULL) AS percentage
              ) AS delivery_fee
            , cs.delivery_time
            , cs.municipality_tax
            , cs.municipality_tax_type
            , cs.status
            , cs.minimum_value
          ) AS settings
        , cs.filters
    ) ORDER BY v.created_at, v.updated_at) AS history
  FROM campaigns ca
  LEFT JOIN `{{ params.project_id }}.dl.porygon_campaigns_versions` v ON ca.country_code = v.country_code
    AND ca.campaign_id = v.id
  LEFT JOIN campaigns_settings cs ON v.country_code = cs.country_code
    AND v.id = cs.id
    AND v.transaction_id = cs.transaction_id
  GROUP BY 1, 2, 3
)
SELECT country_code
  , id
  , ARRAY_AGG(STRUCT(id
      , (SELECT h.operation_type FROM UNNEST(history) h ORDER BY h.active_to DESC, h.transaction_id DESC LIMIT 1) = 'deleted' AS is_deleted
      , history
    )) AS campaigns
FROM campaigns_history
GROUP BY 1, 2
