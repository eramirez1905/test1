WITH billing_logs AS (
  SELECT ReportDate AS date
    , UserId AS user_id
    , ROUND(SUM(TotalCost), 2) AS total_cost
  FROM `{{ params.project_id }}.{{ params.dataset }}.{{ params.view_name }}`
  WHERE ReportDate BETWEEN '{{ ds }}' AND '{{ next_ds }}'
    AND UserId LIKE '%deliveryhero.com'
  GROUP BY 1,2
)
SELECT date
  , user_id
  , total_cost
FROM billing_logs
WHERE total_cost > 30 -- 6 TB per day
ORDER BY total_cost DESC
