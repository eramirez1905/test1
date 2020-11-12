CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._shift_batches`
PARTITION BY created_date AS
WITH base AS (
  SELECT rb.country_code
    , rb.city_id
    , rb.created_at
    , rb.created_date
      -- the day of the week (1-7) on which the batch was created in the system. Because the day_of_week is in local time, first convert
      -- the created_at to local time before converting to day of week for equal comparison
    , EXTRACT(DAYOFWEEK FROM DATETIME(rb.created_at, ci.timezone)) AS created_at_day_number
    , ci.timezone
    , rb.number
    , rb.percentage
    , rb.day_of_week
    , rb.time
    , CASE
        WHEN rb.day_of_week = 'SUNDAY'
          THEN 1
        WHEN rb.day_of_week = 'MONDAY'
          THEN 2
        WHEN rb.day_of_week = 'TUESDAY'
          THEN 3
        WHEN rb.day_of_week = 'WEDNESDAY'
          THEN 4
        WHEN rb.day_of_week = 'THURSDAY'
          THEN 5
        WHEN rb.day_of_week = 'FRIDAY'
          THEN 6
        WHEN rb.day_of_week = 'SATURDAY'
          THEN 7
      ELSE NULL
    -- the day of the week (1-7) on which that batch number can sign up for shifts
    END AS batch_release_day
    , rb.for_newbies
    , rb.for_inactive
    -- select the latest batch rules for each country/city
    , RANK() OVER (PARTITION BY rb.number, rb.country_code, rb.city_id ORDER BY rb.created_at DESC) AS ranking
  FROM `{{ params.project_id }}.dl.rooster_batch` rb
  LEFT JOIN `{{ params.project_id }}.cl.countries` c ON c.country_code = rb.country_code
  LEFT JOIN UNNEST(c.cities) ci ON ci.id = rb.city_id
), first_sign_up_calc AS (
  SELECT country_code
    , city_id
    , created_date
    , batch_release_day
    , number
    , percentage
    , day_of_week AS sign_up_day
    , time AS sign_up_time
    , created_at_day_number
    -- the number of days until the particular batch can sign up for shifts after the
    , ((batch_release_day - created_at_day_number) + 7) AS days_until_first_sign_up
    , for_newbies
    , for_inactive
  FROM base
  WHERE ranking = 1
), first_sign_date AS (
  SELECT country_code
    , city_id
    , number
    , created_date
    , percentage
    , sign_up_day
    , for_newbies
    , for_inactive
    , TIMESTAMP(
        CONCAT(
          DATE_ADD(created_date, INTERVAL days_until_first_sign_up DAY),
          ' ' , sign_up_time
        )
      ) AS first_sign_up_at
  FROM first_sign_up_calc
), sign_up_date_array AS (
  SELECT country_code
    , city_id
    , number
    , GENERATE_TIMESTAMP_ARRAY(
        first_sign_up_at
        , TIMESTAMP_ADD('{{next_execution_date}}', INTERVAL 21 DAY)
        , INTERVAL 7 DAY
    ) AS start_at
  FROM first_sign_date
), create_date_range AS (
  SELECT country_code
    , city_id
    , number
    , start_at
    , TIMESTAMP_SUB(
        LEAD(start_at) OVER (PARTITION BY country_code, city_id, number ORDER BY start_at ASC)
        , INTERVAL 1 SECOND
    ) AS date_end
  FROM sign_up_date_array
  LEFT JOIN UNNEST(start_at) start_at
)

SELECT fsd.country_code
  , fsd.city_id
  , fsd.number AS batch_number
  , fsd.created_date
  , fsd.percentage
  , fsd.sign_up_day
  , fsd.for_newbies
  , fsd.for_inactive
  , ARRAY_AGG(
      STRUCT(
        start_at
        -- use future date in for the last row in each time series. This way we can use date range in join.
        , COALESCE(date_end, TIMESTAMP_ADD('{{next_execution_date}}', INTERVAL 60 DAY)) AS end_at
      )
    ) AS shift_sign_up
FROM first_sign_date fsd
LEFT JOIN create_date_range cdr ON cdr.country_code = fsd.country_code
  AND cdr.city_id = fsd.city_id
  AND cdr.number = fsd.number
GROUP BY 1,2,3,4,5,6,7,8
