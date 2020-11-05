CREATE OR REPLACE TABLE il.absences AS
WITH rooster_absences AS (
  SELECT a.country_code
    , c.id AS city_id
    , a.employee_id AS employment_id
    , CASE
        WHEN a.employee_id IS NULL 
          THEN NULL
        ELSE a.employee_id 
      END AS rider_id
    , DATETIME(a.start_at, c.time_zone) AS stime
    , DATETIME(a.end_at, c.time_zone)  AS etime
    , LOWER(a.type) AS absence_reason
    , LOWER(a.status) AS state
    , NULL AS absence_days
    , paid AS is_paid
    , a.updated_by AS user_id
    , a.created_at
    , a.updated_at
    , a.id AS absence_id
    , c.time_zone
   FROM `{{ params.project_id }}.ml.rooster_absence` a
   LEFT JOIN `{{ params.project_id }}.ml.rooster_employee` ee ON ee.id = a.employee_id
     AND a.country_code = ee.country_code
   LEFT JOIN `{{ params.project_id }}.ml.rooster_employee_contract` ec ON ec.employee_id = a.employee_id
     AND a.country_code = ec.country_code
   LEFT JOIN `{{ params.project_id }}.ml.rooster_city` c on ec.city_id=c.id
     AND ec.country_code = c.country_code
   WHERE LOWER(a.status) IN ('accepted', 'new')
     AND a.start_at >= ec.start_at
     AND a.end_at <= ec.end_at
     AND ec.status IN ('VALID')
), final AS (
  SELECT real_absence_day AS report_date
    , rider_id
    , state
    , is_paid
    , absence_reason
    , employment_id
    , t.country_code
    , IF(CAST(stime AS DATE) = real_absence_day, stime, DATETIME(TIMESTAMP(real_absence_day, t.time_zone), t.time_zone)) AS start_datetime
    , IF(real_absence_day >= CAST(etime AS DATE), etime, DATETIME(TIMESTAMP(DATE_ADD(real_absence_day, INTERVAL 1 DAY), time_zone), time_zone)) AS end_datetime
    , city_id
    , created_at
    , updated_at
    , CONCAT(CAST(absence_id AS STRING), CAST(ROW_NUMBER() OVER(PARTITION BY country_code, employment_id, absence_id ORDER BY created_at) AS STRING)) as absence_id
  FROM rooster_absences t
  LEFT JOIN UNNEST(GENERATE_DATE_ARRAY(CAST(stime AS DATE), CAST(etime AS DATE), INTERVAL 1 DAY)) real_absence_day
)
SELECT country_code
  , city_id
  , employment_id
  , report_date
  , start_datetime AS absence_starts_at
  , end_datetime AS absence_ends_at
  , absence_reason
  , state
  , NULL AS absence_days
  , is_paid
  , employment_id AS user_id
  , rider_id
  , created_at
  , updated_at
  , absence_id
FROM final f
;
                 
