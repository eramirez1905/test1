DROP TABLE `peya-data-origins-stg.origin_data_refined.country_platform_origin`;
CREATE OR REPLACE TABLE
  `peya-data-origins-stg.origin_data_refined.country_platform_origin`
  PARTITION BY RANGE_BUCKET(country_id, GENERATE_ARRAY(0, 20, 1))
  CLUSTER BY country_id, platform_id
	AS 
    SELECT 
       c.id as country_id,
       p.id as platform_id,
       p.name as platform_name
    FROM `peya-data-origins-stg.origins_data_raw.enum_country` c,unnest(platforms) cp
    LEFT JOIN `peya-data-origins-stg.origins_data_raw.enum_platform` as p
    ON cp = p.name;