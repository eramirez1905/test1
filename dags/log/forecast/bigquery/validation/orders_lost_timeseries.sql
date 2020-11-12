SELECT
  SUM(CASE WHEN country_code = 'at' AND event_type = 'shrink' AND datetime = '2018-12-19 19:00:00' THEN orders_lost_net ELSE 0 END) BETWEEN 30 AND 100 AS shrink_at1,
  SUM(CASE WHEN country_code = 'at' AND event_type = 'close' AND datetime = '2019-02-23 18:00:00' THEN orders_lost_net  ELSE 0 END) = 0 AS close_at1,
  SUM(CASE WHEN country_code = 'at' AND event_type = 'close' AND datetime = '2018-03-03 19:00:00' THEN orders_lost_net ELSE 0 END) BETWEEN 50 AND 150 AS close_at2,
  SUM(CASE WHEN country_code = 'ar' AND event_type = 'shrink' AND datetime = '2019-02-23 00:30:00' THEN orders_lost_net ELSE 0 END) BETWEEN 150 AND 400 AS shrink_ar1,
  SUM(CASE WHEN country_code = 'ar' AND event_type = 'close' AND datetime = '2019-03-27 02:00:00' THEN orders_lost_net ELSE 0 END) BETWEEN 200 AND 500 AS close_ar2,
  SUM(CASE WHEN country_code = 'bd' AND event_type = 'shrink' AND datetime = '2019-02-27 12:30:00' THEN orders_lost_net ELSE 0 END) BETWEEN 20 AND 200 AS shrink_bd1,
  SUM(CASE WHEN country_code = 'bd' AND event_type = 'close' AND datetime = '2018-09-25 14:00:00' THEN orders_lost_net ELSE 0 END) BETWEEN 50 AND 200 AS close_bd1,
  COUNTIF(orders_lost_net NOT BETWEEN 0 AND 1E5) = 0 AS orders_lost_net_range,
  -- lost orders should not exceed historic c*orders_max for bigger values 
  COUNTIF(orders_lost_net > 100 AND orders_lost_net > 5 * orders_max) = 0 AS orders_lost_net_reasonable_max,
  SUM(orders_lost_net) > 0 AS orders_lost_net_sum,
  SUM(IF(event_type = 'shrink', orders_lost_net, 0)) > 0 AS orders_shrink_lost_net_sum,
  SUM(IF(event_type = 'shrink_legacy', orders_lost_net, 0)) > 0 AS orders_shrink_legacy_lost_net_sum,
  SUM(IF(event_type = 'close', orders_lost_net, 0)) > 0 AS orders_close_lost_net_sum,
  SUM(IF(event_type = 'outage', orders_lost_net, 0)) > 0 AS orders_outage_lost_net_sum,
  COUNT(DISTINCT CONCAT(CAST(country_code AS STRING), CAST(zone_id AS STRING), CAST(datetime AS STRING), CAST(event_type AS STRING))) = COUNT(*) AS primary_key_condition
FROM forecasting.orders_lost_timeseries ol
LEFT JOIN (
  -- compute rolling max of last x weeks
  SELECT
    country_code,
    zone_id,
    datetime,
    MAX(orders) OVER (
      PARTITION BY country_code, zone_id, EXTRACT(DAYOFWEEK FROM datetime), EXTRACT(HOUR FROM datetime) 
      ORDER BY datetime 
      ROWS BETWEEN 16 PRECEDING AND CURRENT ROW
      ) AS orders_max
  FROM forecasting.orders_timeseries
  ) o
  USING (country_code, zone_id, datetime)
WHERE
    datetime <= '{{next_execution_date}}'
