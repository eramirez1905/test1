drop table `peya-data-origins-stg.origin_data_refined.partner_food_category_origin`;
create table `peya-data-origins-stg.origin_data_refined.partner_food_category_origin`
PARTITION BY RANGE_BUCKET(country_id, GENERATE_ARRAY(0, 20, 1))
cluster by partner_id, food_category_id
as
select p.id                 as partner_id,
       p.country.id         as country_id,
       fc.id                as partner_food_category_id,
       fc.foodCategory.id   as food_category_id
from `peya-data-origins-stg.origins_data_stg.sqs_partner` p
    cross join unnest(p.foodCategories) as fc;