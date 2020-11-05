-- This query is not scheduled on Airflow because is a one-off table
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._vendors_hurrier_fix_vendor_codes` AS
SELECT o.country_code
  , o.vendor.id AS vendor_id
  , ANY_VALUE(SPLIT(o.platform_order_code, '-')[SAFE_OFFSET(0)]) AS vendor_code
  , ANY_VALUE(o.entity.id) AS entity_id
FROM `{{ params.project_id }}.cl.orders` o
INNER JOIN `{{ params.project_id }}.dl.hurrier_businesses` v ON v.country_code = o.country_code
  AND v.id = o.vendor.id
WHERE o.created_date <= '2019-10-01'
GROUP BY 1,2
