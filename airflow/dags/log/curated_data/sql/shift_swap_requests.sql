CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.shift_swap_requests`
PARTITION BY created_date AS
SELECT region
  , country_code
  , created_date
  , created_by
  , shift_id
  , time_zone AS timezone
  , ARRAY_AGG(
      STRUCT(id
        , status
        , accepted_by
        , created_at
        , accepted_at
        , start_at AS shift_start_at
        , end_at AS shift_end_at
        , starting_point_id
  )) AS swap_requests
FROM `{{ params.project_id }}.dl.rooster_shift_swap_request`
GROUP BY 1, 2, 3, 4, 5, 6
