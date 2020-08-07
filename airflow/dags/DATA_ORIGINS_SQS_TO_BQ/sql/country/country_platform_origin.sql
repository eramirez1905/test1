DELETE FROM `peya-data-origins-stg.origin_data_refined.country_platform_origin`
            WHERE platform_id IN (SELECT id
            FROM `peya-data-origins-stg.origins_data_raw.enum_platform`);

INSERT INTO  `peya-data-origins-stg.origin_data_refined.country_platform_origin`
    SELECT 
       c.id as country_id,
       p.id as platform_id,
       p.name as platform_name
    FROM `peya-data-origins-stg.origins_data_raw.enum_country` c,unnest(platforms) cp
    LEFT JOIN `peya-data-origins-stg.origins_data_raw.enum_platform` as p
    ON cp = p.name;