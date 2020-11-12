WITH unnested_results AS (
  SELECT
    results,
    _ingested_at AS _ingested_at_utc,
  FROM `{project_id}.pandata_raw_dynamodb.fp_apac_vci_gaia_rover_scraping_menu_prd`
  CROSS JOIN UNNEST(JSON_EXTRACT_ARRAY(data, '$.result')) AS results
  WHERE DATE(_ingested_at) = DATE('{created_date}')
)

SELECT
  JSON_EXTRACT_SCALAR(items, '$.item_id') AS id,
  JSON_EXTRACT_SCALAR(results, '$.restaurant_id') AS vci_vendor_id,
  JSON_EXTRACT_SCALAR(items, '$.category_id') AS vci_category_id,
  JSON_EXTRACT_SCALAR(results, '$.country_code') AS country_code,
  JSON_EXTRACT_SCALAR(results, '$.source') AS source,
  JSON_EXTRACT_SCALAR(items, '$.item_name') AS item_name,
  JSON_EXTRACT_SCALAR(items, '$.price') AS price_local,
  JSON_EXTRACT_SCALAR(items, '$.discounted_price') AS discounted_price_local,
  JSON_EXTRACT_SCALAR(items, '$.alcohol') AS alcohol_status,
  JSON_EXTRACT_SCALAR(items, '$.popular') AS popular_status,
  JSON_EXTRACT_SCALAR(items, '$.description') AS description,
  JSON_EXTRACT_SCALAR(items, '$.image_url') AS image_url,
  TIMESTAMP_SECONDS(
    CAST(CAST(JSON_EXTRACT_SCALAR(results, '$.timestamp') AS FLOAT64) AS INT64)
  ) AS created_at_utc,
  _ingested_at_utc
FROM unnested_results
CROSS JOIN UNNEST(JSON_EXTRACT_ARRAY(results, '$.items')) AS items
