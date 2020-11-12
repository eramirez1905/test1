WITH job_runs AS
(
  SELECT 
    country_code,
    job_run_id,
    MAX(created_at) AS created_at,
    SUM(orders) AS orders,
  FROM `fulfillment-dwh-production.dl.forecast_orders_forecasts` f 
  WHERE 
    f.created_date >= DATE_SUB(DATE('{{ds}}'), INTERVAL 4 WEEK)
  GROUP BY 1,2
),
job_runs_ranked AS
(
  SELECT
    *,
    -- rank descending to find latest 2 job runs
    ROW_NUMBER() OVER (PARTITION BY country_code ORDER BY job_run_id DESC) AS rank
  FROM job_runs
),
countries AS
(
  SELECT
    c.country_code,
    MAX(order_placed_at) AS last_order_placed_at
  FROM `fulfillment-dwh-production.cl.countries` c
  LEFT JOIN `fulfillment-dwh-production.cl.orders` o USING (country_code)
  WHERE
    -- cl.countries contains countries without country codes
    c.country_code IS NOT NULL
  GROUP BY 1
),
t AS
(
  SELECT
    c.country_code,
    j1.job_run_id AS last_job_run_id,
    TIMESTAMP_TRUNC(j1.created_at, SECOND) AS last_job_run_at,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), j1.created_at, HOUR) AS last_job_run_hours_ago,
    TIMESTAMP_TRUNC(j2.created_at, SECOND) AS previous_job_run_at,
    ROUND((j2.orders - j1.orders) / NULLIF(j2.orders, 0), 4) AS orders_change,
    TIMESTAMP_TRUNC(c.last_order_placed_at, SECOND) AS last_order_placed_at
  FROM countries c
  LEFT JOIN job_runs_ranked j1
    ON j1.country_code = c.country_code
    AND j1.rank = 1
  LEFT JOIN job_runs_ranked j2
    ON j2.country_code = c.country_code
    AND j2.rank = 2
  WHERE
    c.country_code IS NOT NULL
    AND DATE(c.last_order_placed_at) >= DATE_SUB(DATE('{{ds}}'), INTERVAL 4 WEEK)
  ORDER BY 1
)
SELECT
  *
FROM t
WHERE
  -- check for countries with job runs older than 3 days
  last_job_run_hours_ago > 72
