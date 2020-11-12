WITH billing_logs AS (
  SELECT ReportDate AS date
    , UserId AS user_id
    , Table AS table_name
    , ROUND(SUM(TotalCost), 2) AS total_cost
  FROM `{{ params.project_id }}.{{ params.dataset }}.{{ params.view_name }}`
  WHERE ReportDate BETWEEN '{{ ds }}' AND '{{ next_ds }}'
    AND (UserId like '%iam.gserviceaccount.com' OR UserId IN ('metabase', 'tableau'))
  GROUP BY 1,2,3
)
SELECT date
  , CONCAT(user_id, ' on table `', table_name, '`') AS user_id
  , total_cost
FROM billing_logs
WHERE total_cost > 30 -- 6 TB per day
ORDER BY user_id, total_cost DESC
