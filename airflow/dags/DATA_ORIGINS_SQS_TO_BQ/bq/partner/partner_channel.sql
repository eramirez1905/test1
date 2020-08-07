drop table `peya-data-origins-stg.origin_data_refined.partner_channel_origin`;
create table `peya-data-origins-stg.origin_data_refined.partner_channel_origin`
PARTITION BY RANGE_BUCKET(country_id, GENERATE_ARRAY(0, 20, 1))
cluster by partner_id, channel_id
as
select p.id         as partner_id,
       c.id         as channel_id,
       p.country.id as country_id
from `peya-data-origins-stg.origins_data_stg.sqs_partner` p
    cross join unnest(p.channels) as c;