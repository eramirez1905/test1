drop table `peya-data-origins-stg.origin_data_refined.partner_tag_origin`;
create table `peya-data-origins-stg.origin_data_refined.partner_tag_origin`
PARTITION BY RANGE_BUCKET(country_id, GENERATE_ARRAY(0, 18, 1))
cluster by partner_id, tag
as
select p.id             as partner_id,
       p.country.id     as country_id,
       t                as tag
from `peya-data-origins-stg.origins_data_stg.sqs_partner` p
    cross join unnest(tags) as t;