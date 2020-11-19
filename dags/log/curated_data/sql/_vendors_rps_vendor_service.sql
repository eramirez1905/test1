CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._vendors_rps_vendor_service` AS
WITH entities AS (
  SELECT region_short_name AS region
    , p.entity_id
    , p.brand_id
    , p.timezone
    , e.country_iso
  FROM `{{ params.project_id }}.cl.entities` e
  LEFT JOIN UNNEST(platforms) p
), vendor_service_vendors_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.dl.vendor_service_vendors`
), vendor_service_platform_vendor_relations_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.dl.vendor_service_platform_vendor_relations`
), vendor_service_vendors AS (
  SELECT region
    , CAST(id AS INT64) AS vendor_id
    , name AS vendor_name
    , operatorCode AS operator_code
    , active AS is_active
    , LOWER(countryCode) AS country_code
    , UPPER(countryCode) AS country_iso
    , timezone
    , language
    , STRUCT(
        address.street
        , address.building
        , address.zipcode AS postal_code
        , address.city AS city_name
        , SAFE.ST_GEOGPOINT(address.longitude, address.latitude) AS location
      ) AS address
    , version
    , created_at
    , created_date
  FROM vendor_service_vendors_dataset
), vendor_service_platform_vendor_relations AS (
  SELECT region
    , CAST(vendorId AS INT64) AS vendor_id
    , platformGlobalKey AS rps_global_key
    , SPLIT(platformGlobalKey, '_')[OFFSET(0)] AS brand_id
    , platformVendorId AS vendor_code
    , name AS vendor_platform_name
    , enabled AS is_enabled
    , version
    , created_at
    , created_date
  FROM vendor_service_platform_vendor_relations_dataset
), vs_vendors AS (
  SELECT en.region
    , v.vendor_id
    , en.entity_id
    , vr.vendor_code
    , v.vendor_name
    , v.operator_code
    , v.is_active
    , v.country_code
    , en.timezone
    , v.language
    , v.address
    , vr.rps_global_key
    , GREATEST(v.version, vr.version) AS version
    , GREATEST(v.created_at, vr.created_at) AS created_at
    , GREATEST(v.created_date, vr.created_date) AS created_date
    , 'Vendor Service' AS service
  FROM vendor_service_vendors v
  LEFT JOIN vendor_service_platform_vendor_relations vr ON v.vendor_id = vr.vendor_id
    AND v.region = vr.region
  INNER JOIN entities en ON vr.brand_id = en.brand_id
    AND v.country_iso = en.country_iso
), vs_vendors_clean AS (
  SELECT * EXCEPT(_row_number)
  FROM (
    SELECT *
      , ROW_NUMBER() OVER (PARTITION BY entity_id, vendor_code ORDER BY created_at DESC) AS _row_number
    FROM vs_vendors
  )
  WHERE _row_number = 1
)
SELECT UPPER(vs.country_code) AS country_iso
  , vs.region
  , vs.entity_id
  , vs.vendor_code
  , vs.rps_global_key
  , vs.is_active
  , vs.vendor_id
  , vs.vendor_name
  , vs.operator_code
  , vs.timezone
  , vs.address
  , vs.service
  , vs.created_at AS updated_at
  , vs.created_date
  , vs.version
FROM vs_vendors_clean vs
