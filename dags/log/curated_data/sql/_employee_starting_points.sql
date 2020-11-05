CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._employee_starting_points` AS
SELECT sp.country_code
  , sp.employee_id
  , MAX(sp.updated_at) AS updated_at
  , ARRAY_AGG(
      STRUCT(
        sp.starting_points_id AS id
        )
      ) AS starting_points
FROM `{{ params.project_id }}.dl.rooster_employee_starting_points` AS sp
GROUP BY 1, 2
