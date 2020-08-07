DROP TABLE `peya-data-origins-stg.origin_data_refined.country_origin`;
CREATE OR REPLACE TABLE
`peya-data-origins-stg.origin_data_refined.country_origin`
 PARTITION BY RANGE_BUCKET(country_id, GENERATE_ARRAY(0, 50, 1))
	CLUSTER BY country_id, country_short_name, currency_id
	AS 
	SELECT 
      id AS country_id
      ,UPPER(shortName)  as country_code
      ,name	    as country_name
      ,shortName  as country_short_name
      ,currency.id	as currency_id
    FROM `peya-data-origins-stg.origins_data_raw.enum_country`;