SELECT COUNTIF(updated_at IS NULL) = 0 AS riders_updated_is_null
  , COUNTIF(updated_at = '0001-01-01') = 0 AS riders_updated_fake_date
FROM `{{ params.project_id }}.cl.riders`
