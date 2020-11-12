SELECT
  UNIX_DATE(iso_date) AS id,
  iso_date AS date,
  FORMAT_DATE('%Y%m%d', iso_date) AS date_nodash_string,
  MOD(EXTRACT(DAYOFWEEK FROM iso_date) + 5, 7) + 1 AS iso_day_of_week_int,
  FORMAT_DATE('%A', iso_date) AS weekday_name,
  SAFE_CAST(FORMAT_DATE('%V', iso_date) AS INT64) AS iso_week_int,
  FORMAT_DATE('%Y-%V', iso_date) AS iso_year_week_string,
  EXTRACT(DAY FROM iso_date) AS day_of_month_int,
  EXTRACT(MONTH FROM iso_date) AS month_int,
  FORMAT_DATE('%B', iso_date) AS full_month_name,
  FORMAT_DATE('%Y-%m', iso_date) AS year_month_string,
  EXTRACT(YEAR FROM iso_date) AS year_int,
FROM UNNEST(
  GENERATE_DATE_ARRAY(
    '2010-01-01',
    DATE_ADD(CURRENT_DATE, INTERVAL 15 YEAR),
    INTERVAL 1 DAY)
) AS iso_date
