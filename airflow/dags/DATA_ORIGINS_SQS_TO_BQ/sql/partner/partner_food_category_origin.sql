DECLARE FILTER_DATE DATE DEFAULT date('@dateStr');

delete from  `peya-data-origins-stg.origin_data_refined.partner_food_category_origin`
where partner_id in (select id
                        from `peya-data-origins-stg.origins_data_stg.sqs_partner`
                        where yyyymmdd = FILTER_DATE
                     );

insert into `peya-data-origins-stg.origin_data_refined.partner_food_category_origin`
    (
       partner_id,
       country_id,
       partner_food_category_id,
       food_category_id
    )
select p.id                 as partner_id,
       p.country.id         as country_id,
       fc.id                as partner_food_category_id,
       fc.foodCategory.id   as food_category_id
from `peya-data-origins-stg.origins_data_stg.sqs_partner` p
    cross join unnest(p.foodCategories) as fc
where p.yyyymmdd = FILTER_DATE;