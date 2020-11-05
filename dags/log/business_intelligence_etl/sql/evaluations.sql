CREATE OR REPLACE TABLE il.evaluations
PARTITION BY shift_start_date AS
WITH countries AS (
  SELECT co.country_code
    , co.country_name
    , ci.id AS city_id
    , ci.name AS city_name
    , ci.timezone
    , z.id AS zone_id
    , z.name AS zone_name
    , sp.id AS starting_point_id
    , sp.name AS starting_point_name
  FROM `{{ params.project_id }}.cl.countries` co
  LEFT JOIN UNNEST(cities) ci
  LEFT JOIN UNNEST(ci.zones) z
  LEFT JOIN UNNEST(z.starting_points) sp
)
SELECT s.country_code
  , co.city_id
  , co.zone_id
  , s.starting_point_id AS starting_point_id
  , s.id AS shift_id
  , e.id AS evaluation_id
  , s.employee_id AS courier_id
  , CAST(DATETIME(s.start_at, co.timezone) AS DATE) AS shift_start_date
  , DATETIME(e.start_at, co.timezone) AS start_time
  , DATETIME(e.end_at, co.timezone) AS end_time
  , TIMESTAMP_DIFF(e.end_at, e.start_at, MINUTE) AS duration_mins
  , co.timezone
  , CASE
      WHEN co.starting_point_name LIKE '%NO*UTR%'
        THEN TRUE
      ELSE FALSE
    END AS no_utr
  , CAST('working' AS STRING) AS type
  , CASE
      WHEN e.status IS NULL
        AND state IN ('IN_DISCUSSION')
        THEN 'NO_SHOW'
      ELSE state
    END AS to_state
  , CASE
      WHEN state IN ('IN_DISCUSSION')
        THEN TRUE
      ELSE FALSE
    END AS pending
  , s.days_of_week
  , CAST(s.tag AS STRING) AS tag
FROM `{{ params.project_id }}.ml.rooster_shift` s
LEFT JOIN `{{ params.project_id }}.ml.rooster_employee` ee ON s.country_code = ee.country_code
  AND ee.id = s.employee_id
LEFT JOIN `{{ params.project_id }}.ml.rooster_evaluation` e
  ON e.country_code = s.country_code
  AND e.shift_id = s.id
  AND e.status NOT IN ('PENDING')
LEFT JOIN countries co ON s.country_code = co.country_code
    AND s.starting_point_id = co.starting_point_id
WHERE s.state IN ('PUBLISHED', 'IN_DISCUSSION', 'EVALUATED', 'NO_SHOW')
  AND (e.status IN ('PENDING', 'ACCEPTED') OR e.status IS NULL)
  AND e.end_at IS NOT NULL

UNION ALL

SELECT country_code
  , city_id
  , zone_id
  , starting_point_id
  , shift_id
  , evaluation_id
  , courier_id
  , shift_start_date
  , start_time
  , end_time
  , duration_mins
  , timezone
  , no_utr
  , type
  , to_state
  , pending
  , days_of_week
  , tag
FROM (
  SELECT s.country_code
    , co.city_id
    , co.zone_id
    , s.starting_point_id
    , s.id AS shift_id
    , e.id AS evaluation_id
    , s.employee_id AS courier_id
    , CAST(DATETIME(s.start_at, co.timezone) AS DATE) AS shift_start_date
    , DATETIME(e.end_at, co.timezone) AS start_time
    , LAG(DATETIME(e.start_at, co.timezone), 1) OVER (PARTITION BY s.country_code, s.id ORDER BY e.start_at DESC) AS end_time
    , TIMESTAMP_DIFF(LAG(e.start_at, 1) OVER (PARTITION BY s.country_code, s.id ORDER BY e.start_at DESC), e.end_at, MINUTE) AS duration_mins
    , co.timezone
    , CASE
        WHEN co.starting_point_name LIKE '%NO*UTR%'
          THEN TRUE
        ELSE FALSE
      END AS no_utr
    , CAST('break' AS STRING) AS type
    , CASE
        WHEN e.status IS NULL
          AND state IN ('IN_DISCUSSION')
          THEN 'NO_SHOW'
        ELSE state
      END AS to_state
    , CASE
        WHEN state IN ('IN_DISCUSSION')
          THEN TRUE
        ELSE FALSE
      END AS pending
    , s.days_of_week
    , CAST(s.tag AS STRING) AS tag
  FROM `{{ params.project_id }}.ml.rooster_shift` s
  LEFT JOIN `{{ params.project_id }}.ml.rooster_employee` ee ON s.country_code = ee.country_code
    AND ee.id = s.employee_id
  LEFT JOIN `{{ params.project_id }}.ml.rooster_evaluation` e
    ON e.country_code = s.country_code
    AND e.shift_id = s.id
    AND e.status NOT IN ('PENDING')
  LEFT JOIN countries co ON s.country_code = co.country_code
    AND s.starting_point_id = co.starting_point_id
  WHERE s.state IN ('PUBLISHED', 'IN_DISCUSSION', 'EVALUATED', 'NO_SHOW')
    AND (e.status IN ('PENDING', 'ACCEPTED') OR e.status IS NULL)
) f
WHERE end_time IS NOT NULL
;
