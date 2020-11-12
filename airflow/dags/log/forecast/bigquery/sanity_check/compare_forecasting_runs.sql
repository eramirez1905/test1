SELECT
    *
FROM (
    SELECT
        CASE WHEN AVG(avg_orders) > 0
            THEN stddev(avg_orders)/AVG(avg_orders)
            ELSE 0
        END AS stddev_percent_between_job_runs,
        STDDEV(avg_orders) AS stddev,
        AVG(avg_orders) AS avg_orders_per_30_mins,
        zone_id,
        a.country_code,
        z.geo_id,
        z.shape_updated_at,
        ARRAY_AGG(job_run_id) AS job_runs
    FROM (
        SELECT
            AVG(orders) AS avg_orders,
            zone_id,
            country_code,
            created_date,
            job_run_id
        FROM `fulfillment-dwh-production.dl.forecast_orders_forecasts`
        WHERE created_date >= '{{ macros.ds_add(ds, -3) }}'  -- compare forecasts of last 3 days
            AND DATE(forecast_for) > '{{ ds }}'
            AND DATE(forecast_for) <= '{{ macros.ds_add(ds, 7) }}'  -- compare forecast for next week
        GROUP BY
            country_code,
            zone_id,
            created_date,
            job_run_id
        ORDER BY
            country_code,
            zone_id,
            created_date DESC,
            job_run_id DESC
        ) a
    LEFT JOIN
        `fulfillment-dwh-production.dl.porygon_zones` z ON a.country_code = z.country_code AND a.zone_id = z.id
    GROUP BY
        a.zone_id,
        z.geo_id,
        z.shape_updated_at,
        a.country_code
    ) b
WHERE avg_orders_per_30_mins > 4
AND stddev > 0.20
AND stddev_percent_between_job_runs > 0.20
ORDER BY stddev_percent_between_job_runs DESC
