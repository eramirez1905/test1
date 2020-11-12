WITH vendors AS (
  SELECT DISTINCT vendor_code
    , entity_id
  FROM `{{ params.project_id }}.cl.vendors_v2`
), rps_orders AS (
  SELECT DISTINCT vendor.code AS vendor_code
    , entity.id AS entity_id
  FROM `{{ params.project_id }}.cl.rps_orders`
  -- Temp solution until BILOG-883 is done
  WHERE order_id <> 'oma_0e1e6946-08e0-412a-971f-e003142e657c'
), rps_connectivity AS (
  SELECT DISTINCT entity_id
    , vendor_code
  FROM `{{ params.project_id }}.cl.rps_connectivity`
), vendors_all AS (
  SELECT vendor_code
    , entity_id
  FROM vendors
  UNION DISTINCT
  SELECT vendor_code
    , entity_id
  FROM rps_orders
  UNION DISTINCT
  SELECT vendor_code
    , entity_id
  FROM rps_connectivity
), vendors_agg AS (
  SELECT COUNT(*) AS vendors_ct
    , COUNT(v.vendor_code) AS vendors_ct_vendor
  FROM vendors_all va
  LEFT JOIN vendors v ON va.vendor_code = v.vendor_code
    AND va.entity_id = v.entity_id
)
SELECT (vendors_ct = vendors_ct_vendor) AS referential_integrity_vendors_check
FROM vendors_agg
