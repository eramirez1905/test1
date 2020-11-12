CREATE OR REPLACE TABLE il.staffing AS
WITH countries AS (
  SELECT country_code
    , ci.id AS city_id
    , ci.timezone AS timezone
    , ci.name AS city_name
    , zo.id AS zone_id
    , zo.geo_id
    , sp.id
    , sp.name AS starting_point_name
  FROM `{{ params.project_id }}.cl.countries` co
  LEFT JOIN UNNEST(co.cities) ci
  LEFT JOIN UNNEST(ci.zones) zo
  LEFT JOIN UNNEST (zo.starting_points) sp
)
SELECT f.country_code
  , f.planned_schedule_start_time AS start_time
  , f.planned_schedule_end_time AS end_time
  , f.timezone
  , f.state
  , f.days_of_week
  , f.city_id
  , f.starting_point_id
  , f.zone_id
  , f.starting_point_name
  , f.city_name
  , f.tag
  , f.parent_id
  , SUM(f.riders_assigned) AS assigned_shifts
  , SUM(f.riders_unassigned) AS unassigned_shifts
  , SUM(f.riders_assigned) + SUM(f.riders_unassigned) AS shifts_block_size
FROM (
  SELECT s.country_code
    , s.start_at AS planned_schedule_start_time
    , s.end_at AS planned_schedule_end_time
    , sp.timezone
    , s.state
    , CAST(NULL AS STRING) AS days_of_week
    , sp.city_id
    , s.starting_point_id
    , sp.zone_id
    , sp.starting_point_name
    , sp.city_name AS city_name
    , sp.geo_id
    , s.tag
    , CAST(null AS INT64) AS parent_id
    , 'unassigned_shift' AS label
    , SUM(0) AS riders_assigned
    , SUM(slots) AS riders_unassigned
  FROM `{{ params.project_id }}.ml.rooster_unassigned_shift` s
  LEFT JOIN countries sp ON s.country_code = sp.country_code
    AND s.starting_point_id = sp.id
  WHERE sp.starting_point_name NOT LIKE '%NO*UTR%'
    AND s.state NOT IN ('CANCELLED')
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14

UNION ALL

  SELECT s.country_code
    , s.start_at AS planned_schedule_start_time
    , s.end_at AS planned_schedule_end_time
    , sp.timezone
    , s.state
    , ARRAY_TO_STRING(s.days_of_week, "") AS days_of_week
    , sp.city_id
    , s.starting_point_id
    , sp.zone_id
    , sp.starting_point_name
    , sp.city_name
    , sp.geo_id
    , s.tag
    , s.parent_id
    , 'assigned_shift' AS label
    , COUNT(DISTINCT s.employee_id) AS riders_assigned
    , SUM(0) AS riders_unassigned
  FROM `{{ params.project_id }}.ml.rooster_shift` s
  LEFT JOIN `{{ params.project_id }}.ml.rooster_employee` ee ON ee.id = s.employee_id
    AND s.country_code = ee.country_code
  LEFT JOIN countries sp ON s.country_code = sp.country_code
    AND s.starting_point_id = sp.id
  WHERE s.state NOT IN ('CANCELLED')
    AND sp.starting_point_name NOT LIKE '%NO*UTR%'
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
) f
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
;
