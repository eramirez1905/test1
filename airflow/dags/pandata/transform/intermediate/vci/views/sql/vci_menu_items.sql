WITH rank_latest AS (
  SELECT
    *,
    ROW_NUMBER() OVER
      (
        PARTITION BY id, vci_vendor_id, country_code, source
        ORDER BY created_at_utc DESC
      ) = 1
    AS is_latest
  FROM `{project_id}.pandata_intermediate.vci_menu_items_inc`
  WHERE id IS NOT NULL
    AND vci_vendor_id IS NOT NULL
)

SELECT
  id || '_' || vci_vendor_id  || '_' || country_code || '_' || source AS uuid,
  id,
  vci_vendor_id  || '_' || country_code || '_' || source AS vci_vendor_uuid,
  vci_vendor_id,
  vci_category_id  || '_' || country_code || '_' || source AS vci_category_uuid,
  vci_category_id,
  country_code,
  source,
  item_name,
  price_local,
  discounted_price_local,
  SAFE_CAST(alcohol_status AS BOOLEAN) AS is_alcohol,
  CASE
    WHEN popular_status IN ('Yes', 'popular')
    THEN TRUE
    WHEN popular_status IN ('No')
    THEN FALSE
    ELSE CAST(popular_status AS BOOL)
  END AS is_popular,
  description,
  image_url,
  created_at_utc,
  _ingested_at_utc,
FROM rank_latest
WHERE is_latest
