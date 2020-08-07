DECLARE FILTER_DATE DATE DEFAULT date('@dateStr');

delete from  `peya-data-origins-stg.origin_data_refined.partner_areas_origin`
where partner_id in (select id
                        from `peya-data-origins-stg.origins_data_stg.sqs_partner`
                        where yyyymmdd = FILTER_DATE
                     );

insert into `peya-data-origins-stg.origin_data_refined.partner_areas_origin`
    (
       partner_id,
       country_id,
       areas_id
    )
select p.id                 as partner_id,
       p.country.id         as country_id,
       a.id                 as areas_id
from `peya-data-origins-stg.origins_data_stg.sqs_partner` p
    cross join unnest(p.areas) as a
where p.yyyymmdd = FILTER_DATE;