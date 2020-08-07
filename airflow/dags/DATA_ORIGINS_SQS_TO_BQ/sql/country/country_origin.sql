DELETE FROM `peya-data-origins-stg.origin_data_refined.country_origin` 
    WHERE country_id IN (SELECT id 
            FROM `peya-data-origins-stg.origins_data_raw.enum_country`);
            
INSERT INTO `peya-data-origins-stg.origin_data_refined.country_origin` values (0,	'NE',	'Unknown',	'ne',	0);

INSERT INTO `peya-data-origins-stg.origin_data_refined.country_origin` 
    SELECT 
      id AS country_id
      ,UPPER(shortName)  as country_code
      ,name	    as country_name
      ,shortName  as country_short_name
      ,currency.id	as currency_id
    FROM `peya-data-origins-stg.origins_data_raw.enum_country`;