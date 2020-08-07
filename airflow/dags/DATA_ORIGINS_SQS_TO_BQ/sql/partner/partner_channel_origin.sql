DECLARE FILTER_DATE DATE DEFAULT date('@dateStr');

delete from  `peya-data-origins-stg.origin_data_refined.partner_channel_origin`
where partner_id in (select id
                        from `peya-data-origins-stg.origins_data_stg.sqs_partner`
                        where yyyymmdd = FILTER_DATE
                       );

insert into `peya-data-origins-stg.origin_data_refined.partner_channel_origin`
    (
       partner_id,
       channel_id,
       country_id
    )
select p.id         as partner_id,
       c.id         as channel_id,
       p.country.id as country_id
from `peya-data-origins-stg.origins_data_stg.sqs_partner` p
    cross join unnest(p.channels) as c
where p.yyyymmdd = FILTER_DATE;