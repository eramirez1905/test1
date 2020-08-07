DECLARE FILTER_DATE DATE DEFAULT date('@dateStr');

delete from  `peya-data-origins-stg.origin_data_refined.partner_tag_origin`
where partner_id in (select id
                        from `peya-data-origins-stg.origins_data_stg.sqs_partner`
                        where yyyymmdd = FILTER_DATE
                     );

insert into `peya-data-origins-stg.origin_data_refined.partner_tag_origin`
    (
       partner_id,
       country_id,
       tag
    )
select p.id             as partner_id,
       p.country.id     as country_id,
       t                as tag
from `peya-data-origins-stg.origins_data_stg.sqs_partner` p
    cross join unnest(tags) as t
where p.yyyymmdd = FILTER_DATE;