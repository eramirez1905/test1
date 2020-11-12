CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.rps_vendor_attributes_latest` AS
WITH entities AS (
  SELECT en.region_short_name AS region
    , en.country_iso
    , LOWER(en.country_iso) AS country_code
    , en.country_name
    , p.entity_id
    , p.brand_name
    , p.timezone
  FROM `{{ params.project_id }}.cl.entities` en
  CROSS JOIN UNNEST(platforms) p
)
SELECT DISTINCT created_date
  , a.entity_id
  , a.vendor_code
  , a.attributes.vertical_type
  , a.attributes.fixed_vendor_grade AS vendor_grade
  , a.attributes.is_new_vendor
  , IF(a.entity_id IN ('FP_TW','FP_HK'), a.attributes.is_dormant_vendor, FALSE) AS is_dormant_vendor
  , IF(a.entity_id IN ('FP_TW','FP_HK'), a.attributes.is_inoperative_vendor, FALSE) AS is_inoperative_vendor
  , IF(a.entity_id IN ('FP_TW','FP_HK'), a.attributes.is_unengaged_vendor, FALSE) AS is_unengaged_vendor
  , a.attributes.total_orders
  , a.attributes.successful_orders
  , a.attributes.is_latest_record
  , IF(a.entity_id IN ('FP_TW','FP_HK') OR a.attributes.is_ab_vendor, a.is_vm_uo_offlining, TRUE) AS is_vm_uo_offlining
  , IF(a.entity_id IN ('FP_TW','FP_HK') OR a.attributes.is_ab_vendor, a.is_vm_uo_offlining_disabled_reason, NULL) AS is_vm_uo_offlining_disabled_reason
  , IF(c.vendor_code IS NOT NULL,FALSE,a.is_vm_uo_autocall) AS is_vm_uo_autocall
  , IF(c.vendor_code IS NOT NULL,'PH Autocall Test',a.is_vm_uo_autocall_disabled_reason) AS is_vm_uo_autocall_disabled_reason
  , CAST('{{ next_ds }}' AS DATE) AS updated_date
  , DATE('{{ execution_date }}', en.timezone) AS updated_date_local
FROM `{{ params.project_id }}.rl.rps_vendor_attributes` a
INNER JOIN entities en ON a.entity_id = en.entity_id
LEFT JOIN `fulfillment-dwh-staging.devin.ph_autocall_analysis`  c on a.entity_id=c.entity_id and a.vendor_Code=c.vendor_Code and control_pilot='pilot'
WHERE attributes.is_latest_record
