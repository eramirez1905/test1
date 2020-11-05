SELECT
  SUM(CASE WHEN DATE(datetime) = '2019-01-01' THEN orders ELSE 0 END) BETWEEN 127296 * 0.1 AND 127296 * 1.9 AS orders_sum_nyd,
  SUM(CASE WHEN country_code = 'sg' AND zone_id > 0 AND datetime <= '2018-12-31' THEN orders ELSE 0 END) BETWEEN 10003447 * 0.95 AND 10003447 * 1.05 AS orders_sum_sg,
  # more than 100 million orders as of 2019-08-06
  COUNTIF(orders_all NOT BETWEEN 0 AND 100000) = 0 AS orders_all_range,
  COUNTIF(orders_completed NOT BETWEEN 0 AND 100000) = 0 AS orders_completed_range,
  COUNTIF(orders NOT BETWEEN orders_completed AND orders_all) = 0 AS orders_range,
  SUM(orders) > 100000000 AS orders_sum,
  SUM(orders) / SUM(orders_all) > 0.8 AS orders_perc,
  SUM(orders_completed) / SUM(orders_all) > 0.8 AS orders_completed_perc,
  SUM(orders_cancelled) / SUM(orders_all) > 0.001 AS orders_cancelled_perc,
  # at least x% of orders should be assigned to zones
  SUM(CASE WHEN zone_id > 0 THEN orders ELSE 0 END) > 0.95 * SUM(orders) AS orders_zone_perc,
  SUM(tag_preorder) / SUM(orders) > 0.0001 AS tag_preorder_perc,
  SUM(tag_corporate) / SUM(orders) > 0.0001 AS tag_corporate_perc,
  SUM(tag_halal) / SUM(orders) > 0.0001 AS tag_halal_perc,
  SUM(CASE WHEN country_code = 'my' THEN tag_halal ELSE 0 END) / SUM(CASE WHEN country_code = 'my' THEN orders ELSE 0 END) > 0.3 AS tag_halal_perc_my,
  -- checks for 'yesterday'
  AVG(CASE WHEN datetime BETWEEN TIMESTAMP_SUB(TIMESTAMP('{{next_execution_date}}'), INTERVAL 1 DAY) AND TIMESTAMP('{{next_execution_date}}') THEN orders ELSE 0 END) > 0 AS orders_avg_yday,
  -- as of 2019-03-12 we get around 200.000 to 300.000 orders every 24 hours
  SUM(CASE WHEN datetime BETWEEN TIMESTAMP_SUB(TIMESTAMP('{{next_execution_date}}'), INTERVAL 1 DAY) AND TIMESTAMP('{{next_execution_date}}') THEN orders ELSE 0 END) > 100000 AS orders_sum_yday,
  -- check one specific time bin globally
  SUM(CASE WHEN datetime = '2019-02-01 13:00:00' THEN orders ELSE 0 END) BETWEEN 0.1 * (4762+127) AND 1.9 * (4762+127) AS orders_sum_tb,
  SUM(CASE WHEN datetime = '2019-02-01 13:00:00' THEN orders_all ELSE 0 END) BETWEEN 0.1 * 5206 AND 1.9 * 5206 AS orders_all_sum_tb,
  SUM(CASE WHEN datetime = '2019-02-01 13:00:00' THEN orders_completed ELSE 0 END) BETWEEN 0.1 * 4762 AND 1.9 * 4762 AS orders_completed_tb,
  SUM(CASE WHEN datetime = '2019-02-01 13:00:00' THEN orders_cancelled ELSE 0 END) BETWEEN 0.1 * 444 AND 1.9 * 444 AS orders_cancelled_tb,
  SUM(CASE WHEN datetime = '2019-02-01 13:00:00' THEN orders_system_cancelled ELSE 0 END) BETWEEN 0.1 * 317 AND 1.9 * 317 AS orders_system_cancelled_tb,
  SUM(CASE WHEN datetime = '2019-02-01 13:00:00' THEN orders_dispatcher_cancelled ELSE 0 END) BETWEEN 0.1 * 127 AND 1.9 * 127 AS orders_dispatcher_cancelled_tb,
  SUM(CASE WHEN datetime = '2019-02-01 13:00:00' THEN tag_halal ELSE 0 END) BETWEEN 0.1 * 749 AND 1.9 * 749 AS tag_halal_tb,
  SUM(CASE WHEN datetime = '2019-02-01 13:00:00' THEN tag_preorder ELSE 0 END) BETWEEN 0.1 * 187 AND 1.9 * 187 AS tag_preorder_tb,
  SUM(CASE WHEN datetime = '2019-02-01 13:00:00' THEN tag_corporate ELSE 0 END) BETWEEN 0.1 * 5 AND 1.9 * 5 AS tag_corporate_tb,
  COUNT(DISTINCT CONCAT(CAST(country_code AS STRING), CAST(zone_id AS STRING), CAST(datetime AS STRING))) = COUNT(*) AS primary_key_condition
FROM forecasting.orders_timeseries
WHERE
  datetime <= '{{next_execution_date}}'
