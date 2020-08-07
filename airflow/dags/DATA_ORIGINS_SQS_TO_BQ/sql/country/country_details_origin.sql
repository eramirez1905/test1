

DELETE FROM `peya-data-origins-stg.origin_data_refined.country_details_origin`
            WHERE country_id IN (SELECT country_id
            FROM `peya-data-origins-stg.origins_data_raw.gsheet_country_details`);

INSERT INTO  `peya-data-origins-stg.origin_data_refined.country_details_origin`
    SELECT 
            cd.country_id,
            cd.country_name,
            cd.country_name_en,
            cd.brand_name,
            cd.url_peya,
            cd.url_root_image_cat,
            cd.url_root_images,
            cd.url_root_restaurant,
            cd.generic_food_image,
            cd.currency_price,
            cd.phone_dialing_code,
            cd.brand ,
            cd.adwords_location_id,
            CASE WHEN (cd.active_from IS NULL OR cd.active_to IS NULL) THEN FALSE 
                WHEN (CURRENT_DATE() BETWEEN CAST(cd.active_from AS datetime) AND cast(cd.active_to as datetime)) THEN TRUE 
                ELSE FALSE 
            END as active,
            cd.matrix_visible,
            cast(replace(cd.percentage_tax,',','.')as FLOAT64) as percentage_tax,
            cast(replace(cd.percentage_tax_logistics,',','.')as FLOAT64) as percentage_tax_logistics,
            CASE WHEN cd.country_id = 17 THEN 0 else cast(replace(cd.percentage_tax_logistics,',','.')as FLOAT64)  END as percentage_tax_marketing,
            NULL as migration_id,
            cd.timezone,
            c.timeFormat as time_format,
            c.defaultCity.id AS default_city_id,
            c.culture as culture,
            ic.id AS  identity_card_behaviour,
            CAST(CASE 
                WHEN cd.country_id = 1 then '2014-12-04 00:00:00'
                WHEN cd.country_id = 2 then '2014-12-04 00:00:00'
                WHEN cd.country_id = 3 then '2014-12-04 00:00:00'
                WHEN cd.country_id = 4 then '2014-12-04 00:00:00'
                WHEN cd.country_id = 5 then '2014-12-04 00:00:00'
                WHEN cd.country_id = 6 then '2014-12-04 00:00:00'
                WHEN cd.country_id = 7 then '2014-12-04 00:00:00'
                WHEN cd.country_id = 8 then '2014-12-04 00:00:00'
                WHEN cd.country_id = 9 then '2014-12-04 00:00:00'
                WHEN cd.country_id = 11 then '2014-12-04 00:00:00'
                WHEN cd.country_id = 13 then '2014-12-04 00:00:00'
                WHEN cd.country_id = 15 then '2014-12-04 00:00:00'
                WHEN cd.country_id = 16 then '2017-12-20 00:00:00'
                WHEN cd.country_id = 17 then '2018-04-26 00:00:00'
                WHEN cd.country_id = 18 then '2019-09-03 00:00:00'
                ELSE NULL 
            END AS TIMESTAMP) AS date_created,
            cd.iso_code_2,
            cd.iso_code_3,
            cd.url_background_generic_images,
            cd.url_background_custom_images,
            cd.url_product_images,
            cd.url_category_images,
            CAST(cd.active_from AS date) AS active_from,
            CAST(cd.active_to AS date) as active_to

        FROM `peya-data-origins-stg.origins_data_raw.gsheet_country_details`  AS cd 
        LEFT JOIN  `peya-data-origins-stg.origins_data_raw.enum_country` AS c ON c.id=cd.country_id 
        LEFT JOIN  `peya-data-origins-stg.origins_data_raw.enum_identity_card_behaviour` AS ic ON UPPER(ic.name)=UPPER(c.identityCardBehaviour) ;