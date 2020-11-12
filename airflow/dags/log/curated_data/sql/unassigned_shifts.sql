CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.unassigned_shifts`
PARTITION BY created_date AS
WITH countries AS (
  SELECT c.country_code
    , cc.id AS city_id
    , cc.timezone
    , z.id AS zone_id
    , s.id AS starting_point_id
  FROM `{{ params.project_id }}.cl.countries` AS c
  LEFT JOIN unnest(cities) AS cc
  LEFT JOIN unnest(zones) AS z
  LEFT JOIN unnest(starting_points) AS s
)
SELECT c.country_code
  , r.created_date
  , c.city_id
  , c.zone_id
  , r.starting_point_id
  , r.id AS unassigned_shift_id
  , r.slots
  , r.start_at
  , r.end_at
  , r.created_at
  , r.updated_at
  , r.created_by
  , r.updated_by
  , r.tag
  , r.state
  , c.timezone
FROM `{{ params.project_id }}.dl.rooster_unassigned_shift` AS r
LEFT JOIN countries AS c ON r.country_code = c.country_code
  AND r.starting_point_id = c.starting_point_id
