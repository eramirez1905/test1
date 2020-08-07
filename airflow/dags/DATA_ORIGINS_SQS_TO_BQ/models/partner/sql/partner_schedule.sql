select restaurant_id,
                case when n_day_weekday + n_day_weekend > 0 then 1 else 0 end   as day,
                n_day_weekday,
                n_day_weekend,
                case when n_night_weekday + n_night_weekend > 0 then 1 else 0 end as night,
                n_night_weekday,
                n_night_weekend
from (
SELECT -- distinct restaurant_id, open_from_hour, open_to_hour, open_from_date, open_to_date
restaurant_id,
                count(distinct case when extract(hour from open_from_date) <= 13 and extract(hour from
 open_to_date) +
                                (case when extract(day from open_to_date) > 1 then (extract(day from open_to_date)-1)*24 else
 1 end)
                                                        >= 13 and
                                                        schedule_day_id in(2, 3, 4, 5) then schedule_day_id end) as n_day_weekday,
                count(distinct case when extract(hour from open_from_date) <= 13 and extract(hour from
 open_to_date) +
                                (case when extract(day from open_to_date) > 1 then (extract(day from open_to_date)-1)*24 else
 1 end)
                                                        >= 13 and
                                                        schedule_day_id in(6, 7, 1) then schedule_day_id end) as n_day_weekend,
                count(distinct case when extract(hour from open_from_date) <= 23 and extract(hour from
 open_to_date) +
                                (case when extract(day from open_to_date) > 1 then (extract(day from open_to_date)-1)*24 else
 1 end)
                                                        > 21 and
                                                        schedule_day_id in(2, 3, 4, 5) then schedule_day_id end) as n_night_weekday,
                count(distinct case when extract(hour from open_from_date) <= 23 and extract(hour from
 open_to_date) +
                                (case when extract(day from open_to_date) > 1 then (extract(day from open_to_date)-1)*24 else
 1 end)
                                                        > 21 and
                                                        schedule_day_id in(6, 7, 1) then schedule_day_id end) as n_night_weekend
FROM
(
(
select distinct restaurant_id, schedule_day_id, open_from_date, open_to_date
from dim_restaurant_schedule
where restaurant_schedule_id not in(

                        select distinct restaurant_schedule_id
                        from dim_restaurant_schedule s
                                inner join
                                (
                                        select restaurant_id, schedule_day_id, hour, minute
                                        from
                                        (
                                                select restaurant_id,
                                                                schedule_day_id,
                                                                extract(hour from open_from_date) as hour,
                                                                extract(minute from open_from_date) as minute
                                                from dim_restaurant_schedule s1
                                                -- where restaurant_id = 33870
                                        UNION ALL
                                                select restaurant_id,
                                                                (case when extract(day from open_to_date) > 1 then schedule_day_id + 1 else
 schedule_day_id end) as schedule_day_id,
                                                                extract(hour from open_to_date) as hour,
                                                                extract(minute from open_to_date) as minute
                                                from dim_restaurant_schedule s2
                                                -- where restaurant_id = 33870
                                        ) a
                                        group by restaurant_id, schedule_day_id, hour, minute
                                        having count(*) > 1
                                ) b
                                on s.restaurant_id = b.restaurant_id and
                                                        ((b.hour = open_from_hour and
                                                        b.minute = extract(minute from open_from_date) and
                                                        s.schedule_day_id = b.schedule_day_id)
                                                or
                                                        (b.hour = open_to_hour and
                                                        b.minute = extract(minute from open_to_date) and
                                                        (case when extract(day from s.open_to_date) > 1 then s.schedule_day_id + extract(day from
 s.open_to_date) - 1
                                                                else s.schedule_day_id end) = b.schedule_day_id))
                                )

)
UNION ALL
(

select restaurant_id, -- schedule_day, hour, minute,
                min(schedule_day_id) as schedule_day_id,
                min(open_from_date) as open_from_date,
                dateadd(day, sum(dif), dateadd(day, -extract(day from max(open_to_date)) + 1,
  max(open_to_date))) as open_to_date
from
(
        select
                        b.restaurant_id,
                        b.schedule_day_id as schedule_day,
                        b.hour,
                        b.minute,
                                case when b.hour = s.open_to_hour and
                                                        b.minute = extract(minute from s.open_to_date) and
                                                        s.schedule_day_id =
                                                                        case when b.schedule_day_id - extract(day from open_to_date) + 1 <= 0
                                                                                then b.schedule_day_id - extract(day from open_to_date) + 8
                                                                                else b.schedule_day_id - extract(day from open_to_date) + 1 end
                                then s.schedule_day_id
                                end as schedule_day_id,
                                case when b.hour = s.open_to_hour and
                                                        b.minute = extract(minute from s.open_to_date) and
                                                        s.schedule_day_id =
                                                                        case when b.schedule_day_id - extract(day from open_to_date) + 1 <= 0
                                                                                then b.schedule_day_id - extract(day from open_to_date) + 8
                                                                                else b.schedule_day_id - extract(day from open_to_date) + 1 end
                                then s.open_from_date
                                end as open_from_date,
                                case when b.hour = s.open_from_hour and
                                                        b.minute = extract(minute from s.open_from_date) and
                                                        s.schedule_day_id = b.schedule_day_id
                                then s.open_to_date
                                end as open_to_date,
                                datediff(day, open_from_date, open_to_date) as dif
        from dim_restaurant_schedule s
        inner join
        (
                select restaurant_id, schedule_day_id, hour, minute
                from
                (
                        select restaurant_id,
                                        schedule_day_id,
                                        extract(hour from open_from_date) as hour,
                                        extract(minute from open_from_date) as minute
                        from dim_restaurant_schedule s1
                        -- where restaurant_id = 33870
                UNION ALL
                        select restaurant_id,
                                        (case when extract(day from open_to_date) > 1 then schedule_day_id + 1 else schedule_day_id
 end) as schedule_day_id,
                                        extract(hour from open_to_date) as hour,
                                        extract(minute from open_to_date) as minute
                        from dim_restaurant_schedule s2
                        -- where restaurant_id = 33870
                ) a
                group by restaurant_id, schedule_day_id, hour, minute
                having count(*) > 1
        ) b
        on s.restaurant_id = b.restaurant_id and
                                ((b.hour = open_from_hour and
                                b.minute = extract(minute from open_from_date) and
                                s.schedule_day_id = b.schedule_day_id)
                        or
                                (b.hour = open_to_hour and
                                b.minute = extract(minute from open_to_date) and
                                (case when extract(day from s.open_to_date) > 1 then s.schedule_day_id + extract(day from
 s.open_to_date) - 1
                                        else s.schedule_day_id end) = b.schedule_day_id))
        -- where s.restaurant_id = 33870
) c
group by restaurant_id, schedule_day, hour, minute
having
min(schedule_day_id) is not null and
min(open_from_date) is not null and
dateadd(day, sum(dif), dateadd(day, -extract(day from max(open_to_date)) + 1,
  max(open_to_date))) is not null
)

) y
group by restaurant_id ) x