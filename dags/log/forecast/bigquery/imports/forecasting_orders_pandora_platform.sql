-- create partitioned legacy event table from imported data
CREATE OR REPLACE TABLE forecasting.orders_pandora_platform
PARTITION BY created_date
CLUSTER BY country_code AS
SELECT
	country_code,
	city_id,
	date,
	orders,
	orders_completed,
	orders_cancelled,
	tags_preorder,
	tags_corporate,
	date AS created_date
FROM `dh-logistics-data-science.imports.forecasting_orders_pandora_platform_json`
ORDER BY 1,2,3
