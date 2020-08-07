drop table `peya-data-origins-stg.origin_data_refined.partner_areas_origin`;
create table `peya-data-origins-stg.origin_data_refined.partner_areas_origin`
PARTITION BY RANGE_BUCKET(country_id, GENERATE_ARRAY(0, 20, 1))
cluster by partner_id, areas_id
as
select p.id                 as partner_id,
       p.country.id         as country_id,
       a.id                 as areas_id
from `peya-data-origins-stg.origins_data_stg.sqs_partner` p
    cross join unnest(p.areas) as a;