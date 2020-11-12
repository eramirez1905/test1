CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_dmart.warehouses` AS
WITH successful_first_order_per_dmart AS (
  SELECT country_code
    , warehouse_id
    , SUBSTR(name, 1, 4) AS vendor_code
    , DATE(MIN(date_order)) AS first_order_date
  FROM `{{ params.project_id }}.dl_dmart.odoo_sale_order`
  WHERE state = 'done'
  GROUP BY country_code
    , warehouse_id
    , vendor_code
),

dmart_info AS (
  SELECT company.entities AS entity_id
    , UPPER(company.country_code) AS country_code
    , successful_first_order_per_dmart.warehouse_id
    , company.name AS warehouse_name
    , successful_first_order_per_dmart.vendor_code
    , currency.name AS currency_code
    , successful_first_order_per_dmart.first_order_date
  FROM successful_first_order_per_dmart
  LEFT JOIN `{{ params.project_id }}.dl_dmart.odoo_res_company` AS company
    ON successful_first_order_per_dmart.country_code = company.country_code
    AND successful_first_order_per_dmart.warehouse_id = company.id
  LEFT JOIN `{{ params.project_id }}.dl_dmart.odoo_res_currency` AS currency
    ON successful_first_order_per_dmart.country_code = currency.country_code
    AND company.currency_id = currency.id
  WHERE company.name LIKE '%Panda%'
)

SELECT entity_id
  , country_code
  , warehouse_id
  , warehouse_name
  , vendor_code
  , currency_code
  , first_order_date AS active_at
  , DATE_DIFF('{{ next_ds }}', first_order_date, MONTH) AS age_in_months
  , DATE_DIFF('{{ next_ds }}', first_order_date, WEEK) AS age_in_weeks
  , DATE_DIFF('{{ next_ds }}', first_order_date, DAY) AS age_in_days
FROM dmart_info
