WITH vendors AS (
  SELECT
    (
      JSON_EXTRACT_SCALAR(data, '$.result[0].restaurant_id') || '_' ||
      JSON_EXTRACT_SCALAR(data, '$.result[0].country_code') || '_' ||
      JSON_EXTRACT_SCALAR(data, '$.result[0].source')
    ) AS uuid,
    JSON_EXTRACT_SCALAR(data, '$.result[0].restaurant_id') AS id,
    UPPER(
      JSON_EXTRACT_SCALAR(data, '$.result[0].country_code')
    ) AS country_code,
    JSON_EXTRACT_SCALAR(data, '$.result[0].source') AS source,
    JSON_EXTRACT_SCALAR(data, '$.result[0].name') AS name,
    JSON_EXTRACT_SCALAR(data, '$.result[0].entity_type') AS vendor_type,
    JSON_EXTRACT_SCALAR(data, '$.result[0].restaurant_description') AS description,
    JSON_EXTRACT_SCALAR(data, '$.result[0].address') AS address,
    JSON_EXTRACT_SCALAR(data, '$.result[0].postal_code') AS postal_code,
    JSON_EXTRACT_SCALAR(data, '$.result[0].city') AS city,
    JSON_EXTRACT_SCALAR(data, '$.result[0].area') AS area,
    JSON_EXTRACT_SCALAR(data, '$.result[0].phone_number') AS phone_number,
    JSON_EXTRACT_SCALAR(data, '$.result[0].restaurant_email') AS email,
    ARRAY(
      SELECT JSON_EXTRACT_SCALAR(cuisine_types, '$') AS cuisine_type
      FROM UNNEST(JSON_EXTRACT_ARRAY(data, '$.result[0].cuisine_type')) AS cuisine_types
    ) AS cuisine_types,
    JSON_EXTRACT_SCALAR(data, '$.result[0].comission_per_order') AS commission_rate,
    SAFE_CAST(
      JSON_EXTRACT_SCALAR(data, '$.result[0].halal') AS BOOL
    ) AS has_halal_options,
    SAFE_CAST(
      JSON_EXTRACT_SCALAR(data, '$.result[0].is_free_delivery') AS BOOL
    ) AS has_free_delivery,
    JSON_EXTRACT_SCALAR(data, '$.result[0].minimum_order_price') AS minimum_order_price_local,
    JSON_EXTRACT_SCALAR(data, '$.result[0].delivery_fee') AS delivery_fee_local,
    JSON_EXTRACT_SCALAR(data, '$.result[0].rating') AS rating,
    JSON_EXTRACT_SCALAR(data, '$.result[0].number_of_reviews') AS number_of_reviews,
    JSON_EXTRACT_SCALAR(data, '$.result[0].opening_hours') AS opening_hours,
    JSON_EXTRACT_SCALAR(data, '$.result[0].delivery_time') AS estimated_delivery_time_in_minutes,
    TIMESTAMP_SECONDS(
      CAST(CAST(JSON_EXTRACT_SCALAR(data, '$.result[0].timestamp') AS FLOAT64) AS INT64)
    ) AS created_at_utc,
  FROM `{project_id}.pandata_raw_dynamodb.fp_apac_vci_gaia_rover_scraping_restaurant_prd`
)

SELECT
  * EXCEPT (is_latest),
  DATE(created_at_utc) AS created_date_utc,
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY uuid
      ORDER BY created_at_utc DESC
    ) = 1
    AS is_latest
  FROM vendors
  WHERE id IS NOT NULL
)
WHERE is_latest
