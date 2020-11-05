SELECT
  COUNTIF(value_mean IS NULL) = 0 AS value_mean_not_null,
  COUNTIF(value_mean_unweighted  IS NULL) = 0  AS value_mean_unweighted_not_null,
  COUNTIF(value_median IS NULL) = 0  AS value_median_not_null,
  COUNTIF(value_min IS NULL) = 0  AS value_min_not_null,
  COUNTIF(value_max IS NULL) = 0  AS value_max_not_null,
  COUNTIF(value_max < value_min) = 0  AS value_max_min_range,
  -- avg number of events per half hour (for the time bins where an event happened should be >1 and reasonably small)
  AVG(ARRAY_LENGTH(event_ids)) BETWEEN 1 AND 10 AS event_ids_avg_length_range,
  COUNT(DISTINCT CONCAT(CAST(country_code AS STRING), CAST(zone_id AS STRING), CAST(datetime AS STRING), CAST(event_type AS STRING))) = COUNT(*) AS primary_key_condition,
  -- porygon events
  COUNTIF(country_code = 'sg' AND zone_id > 0 AND datetime = '2019-02-21 23:00:00' AND event_type = 'close' AND duration >= 25) > 0 AS close1a,
  COUNTIF(country_code = 'sg' AND zone_id > 0 AND datetime = '2019-02-21 23:30:00' AND event_type = 'close' AND duration >= 3 ) > 0 AS close1b,
  COUNTIF(country_code = 'sg' AND zone_id > 0 AND datetime = '2018-12-18 16:30:00' AND event_type = 'close' AND duration >= 30) > 0 AS close2,
  COUNTIF(country_code = 'no' AND zone_id > 0 AND datetime = '2019-01-07 20:00:00' AND event_type = 'close') = 0 AS close3a,
  COUNTIF(country_code = 'no' AND zone_id > 0 AND datetime = '2019-01-07 20:30:00' AND event_type = 'close' AND duration >= 14) > 0 AS close3b,
  COUNTIF(country_code = 'at' AND zone_id > 0 AND datetime = '2019-02-23 12:00:00' AND event_type = 'shrink') = 0 AS shrink1a,
  COUNTIF(country_code = 'at' AND zone_id > 0 AND datetime = '2019-02-23 12:30:00' AND event_type = 'shrink' AND duration >= 12 AND value_min <= 10 AND value_max >= 10) > 0 AS shrink1b,
  COUNTIF(country_code = 'at' AND zone_id > 0 AND datetime = '2019-02-23 13:00:00' AND event_type = 'shrink' AND duration >= 22 AND value_min <= 10 AND value_max >= 10) > 0 AS shrink1c,
  COUNTIF(country_code = 'at' AND zone_id > 0 AND datetime = '2019-02-23 13:30:00' AND event_type = 'shrink') = 0 AS shrink1d,
  -- pandora legacy events
  -- Toronto Center delay
  -- Singapore Houangsen close
  COUNTIF(country_code = 'sg' AND zone_id > 0 AND datetime = '2017-10-01 09:30:00' AND event_type = 'close') = 0 AS legacy_delay3a,
  -- TODO: improve events_timeseries such that value_min=0 for the below cases
  COUNTIF(country_code = 'sg' AND zone_id > 0 AND datetime = '2017-10-01 10:00:00' AND event_type = 'close' AND duration >= 4 AND value_max >= 1) > 0 AS legacy_delay3b,
  COUNTIF(country_code = 'sg' AND zone_id > 0 AND datetime = '2017-10-01 10:30:00' AND event_type = 'close' AND duration >= 13 AND value_max >= 1) > 0 AS legacy_delay3c,
  -- Uppsala shrink_legacy
  COUNTIF(country_code = 'se' AND zone_id > 0 AND datetime = '2018-07-22 15:30:00' AND event_type = 'shrink_legacy' AND duration >= 14 AND value_mean > 0) > 0 AS legacy_shrink1a,
  COUNTIF(country_code = 'se' AND zone_id > 0 AND datetime = '2018-07-22 15:00:00' AND event_type = 'shrink_legacy') = 0 AS legacy_shrink1b,
  -- outages:
  -- RPS outage
  COUNTIF(country_code = 'hk' AND datetime = '2019-03-01 07:00:00' AND event_type = 'outage' AND ABS(duration - 30) < 1.5 AND value_mean = 1) > 1 AS outage1,
  -- checks for 'yesterday'
  AVG(CASE WHEN datetime BETWEEN TIMESTAMP_SUB(TIMESTAMP('{{next_execution_date}}'), INTERVAL 1 DAY) AND TIMESTAMP('{{next_execution_date}}') THEN duration ELSE 0 END) > 0 AS duration_avg_yday,
  AVG(CASE WHEN datetime BETWEEN TIMESTAMP_SUB(TIMESTAMP('{{next_execution_date}}'), INTERVAL 1 DAY) AND TIMESTAMP('{{next_execution_date}}') THEN value_mean ELSE 0 END) > 0 AS value_mean_avg_yday
FROM forecasting.events_timeseries
