CREATE OR REPLACE TABLE il.sr_open_slots_dataset
PARTITION BY report_date_local AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 5 WEEK) AS start_date
    , DATE_ADD('{{ next_ds }}', INTERVAL 2 WEEK) AS end_date
), dates AS (
  SELECT date_time
  FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(
    TIMESTAMP_TRUNC(TIMESTAMP_SUB((SELECT CAST(end_date AS TIMESTAMP) FROM parameters), INTERVAL 50 DAY), DAY),
    TIMESTAMP_TRUNC((SELECT CAST(end_date AS TIMESTAMP) FROM parameters), HOUR), INTERVAL 15 MINUTE)
  ) AS date_time
), report_dates AS (
  SELECT CAST(date_time AS DATE) AS report_date
    , date_time AS start_datetime
    , TIMESTAMP_ADD(date_time, INTERVAL 15 MINUTE) AS end_datetime
  FROM dates
)
SELECT DISTINCT s.country_code
  , s.city_id
  , CAST(DATETIME(dates.start_datetime, s.timezone) AS DATE) AS report_date_local
  , DATETIME(dates.start_datetime, s.timezone) AS start_datetime_local
  , DATETIME(dates.end_datetime, s.timezone) AS end_datetime_local
  , zone_id
  , timezone
  , SUM(IF((s.days_of_week IS NULL OR UPPER(FORMAT_DATE("%A", DATE(s.start_time))) = s.days_of_week), s.assigned_shifts, 0)) + SUM(s.unassigned_shifts) AS open_slots_size
  , SUM(IF((s.days_of_week IS NULL OR UPPER(FORMAT_DATE("%A", DATE(s.start_time))) = s.days_of_week), s.assigned_shifts, 0)) AS open_slots_assigned
  , SUM(s.unassigned_shifts) AS open_slots_unassigned
  , SUM(IF(s.tag IN ('SWAP') AND s.parent_id IS NULL, s.assigned_shifts, NULL)) + SUM(IF(s.tag IN ('SWAP'), s.unassigned_shifts, NULL)) AS swap_open_slots
  , SUM(IF(s.tag IN ('MANUAL') AND s.parent_id IS NULL, s.assigned_shifts, NULL)) + SUM(IF(s.tag IN ('MANUAL'), s.unassigned_shifts, NULL)) AS manual_slots_size
  , SUM(IF(s.tag IN ('APPLICATION'), s.assigned_shifts, NULL)) + SUM(IF(s.tag IN ('APPLICATION'), s.unassigned_shifts, NULL)) AS application_slots_size
  , SUM(IF(s.tag IN ('HURRIER'), s.assigned_shifts, NULL)) + SUM(IF(s.tag IN ('HURRIER'), s.unassigned_shifts, NULL)) AS hurrier_slots_size
  , SUM(IF(s.tag IN ('AUTOMATIC'), s.assigned_shifts, NULL)) + SUM(IF(s.tag IN ('AUTOMATIC'), s.unassigned_shifts, NULL)) AS automatic_slots_size
  , SUM(IF(s.tag IN ('MANUAL') OR s.tag IN ('SWAP') AND s.parent_id IS NOT NULL, s.assigned_shifts, NULL)) + SUM(IF(s.tag IS NULL, s.assigned_shifts, NULL)) AS repeating_slots_size
FROM il.staffing s
INNER JOIN report_dates dates ON s.end_time >= dates.end_datetime
  AND s.start_time <= dates.start_datetime
  -- the following condition is needed as repeating shifts even if satisfying the previous condition they end far in
  -- the future so they are overestimated in the 15 mins intervals.
  AND IF(CAST(DATETIME(s.start_time, s.timezone) AS DATE) > '{{ next_ds }}' AND s.days_of_week IS NOT NULL AND s.state = 'PENDING', TIME(s.end_time) > TIME(dates.end_datetime) AND TIME(s.start_time) < TIME(dates.start_datetime), TRUE)
WHERE CAST(DATETIME(s.start_time, s.timezone) AS DATE) BETWEEN (SELECT start_date FROM parameters) AND (SELECT end_date FROM parameters)
GROUP BY 1 , 2, 3, 4, 5, 6, 7

