WITH unnested_results AS (
  SELECT
    results,
    _ingested_at AS _ingested_at_utc,
  FROM `{project_id}.pandata_raw_dynamodb.fp_apac_vci_gaia_rover_scraping_menu_prd`
  CROSS JOIN UNNEST(JSON_EXTRACT_ARRAY(data, '$.result')) AS results
  WHERE DATE(_ingested_at) = DATE('{created_date}')
)

SELECT
  JSON_EXTRACT_SCALAR(categories, '$.category_id') AS id,
  JSON_EXTRACT_SCALAR(unnested_results.results, '$.country_code') AS country_code,
  JSON_EXTRACT_SCALAR(unnested_results.results, '$.source') AS source,
  JSON_EXTRACT_SCALAR(categories, '$.name') AS name,
  JSON_EXTRACT_SCALAR(categories, '$.description') AS description,
  TIMESTAMP_SECONDS(
    CAST(CAST(JSON_EXTRACT_SCALAR(results, '$.timestamp') AS FLOAT64) AS INT64)
  ) AS created_at_utc,
  _ingested_at_utc,
FROM unnested_results
CROSS JOIN UNNEST(JSON_EXTRACT_ARRAY(unnested_results.results, '$.category')) AS categories
