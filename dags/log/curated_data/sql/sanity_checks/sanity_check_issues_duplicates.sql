WITH issues AS (
  SELECT country_code
    , issue_id
    , COUNT(*) AS count
  FROM `{{ params.project_id }}.cl.issues`
  GROUP BY 1, 2
)
SELECT COUNTIF(count != 1) = 0 AS duplicates_count
FROM issues
