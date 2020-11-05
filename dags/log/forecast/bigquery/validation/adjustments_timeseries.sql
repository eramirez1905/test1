SELECT
  COUNTIF(country_code = 'at' AND datetime BETWEEN '2019-04-29 23:00:00' AND '2019-04-30 22:30:00') = 0 AS adjustment_labourday1,
  COUNTIF(country_code = 'at' AND datetime BETWEEN '2019-04-30 23:00:00' AND '2019-05-01 22:30:00' AND reason = 'special_day') >= 48 AS adjustment_labourday2
FROM forecasting.adjustments_timeseries
WHERE
  datetime <= '{{next_execution_date}}'
