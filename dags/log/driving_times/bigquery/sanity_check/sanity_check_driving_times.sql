WITH accepted_to_pickup AS (
  SELECT
    d.vehicle.profile AS vehicle_profile,
    country_code,
    "cl.orders.city_id",
    DATE(d.rider_accepted_at) AS date,
    'accepted_to_pickup' AS type,
    TIMESTAMP_DIFF(d.rider_near_restaurant_at, dt.estimated_pickup_arrival_at, SECOND) AS error
  FROM
    `cl.orders`
    JOIN UNNEST(deliveries) d
    JOIN UNNEST(d.transitions) dt
    LEFT JOIN `cl.countries` USING (country_code)
  WHERE
    order_status = 'completed'
    AND d.vehicle.profile IS NOT NULL
    AND dt.state = 'accepted'
    AND created_date > '{{ macros.ds_add(ds, -7) }}'
    AND created_date <= '{{ ds }}'
),
pickup_to_dropoff AS (
  SELECT
    d.vehicle.profile AS vehicle_profile,
    country_code,
    "cl.orders.city_id",
    DATE(d.rider_accepted_at) AS date,
    'pickup_to_dropoff' AS type,
    TIMESTAMP_DIFF(d.rider_near_customer_at, dt.estimated_dropoff_arrival_at, SECOND) AS error
  FROM
    `cl.orders`
    JOIN UNNEST(deliveries) d
    JOIN UNNEST(d.transitions) dt
    LEFT JOIN `cl.countries` USING (country_code)
  WHERE
    order_status = 'completed'
    AND d.vehicle.profile IS NOT NULL
    AND dt.state = 'left_pickup'
    AND created_date > '{{ macros.ds_add(ds, -7) }}'
    AND created_date <= '{{ ds }}'
),
all_errors AS (
  SELECT
    *
  FROM (
    SELECT * FROM accepted_to_pickup
    UNION ALL
    SELECT * FROM pickup_to_dropoff
  )
  WHERE
    ABS(error) < 7200
),
past_mean_error AS (
  SELECT
    vehicle_profile,
    country_code,
    AVG(ABS(error)) AS mae
  FROM
    all_errors
  GROUP BY 1, 2
),
result AS (
  SELECT
    vehicle_profile,
    country_code,
    date,
    COUNT(*) as num_datapoints,
    ROUND((AVG(ABS(error)) / AVG(pme.mae) - 1.0) * 100, 1) AS mae_change_pct
  FROM
    all_errors
    LEFT JOIN past_mean_error pme USING (vehicle_profile, country_code)
  WHERE
    date = '{{ ds }}'
  GROUP BY
    1, 2, 3
  HAVING COUNT(*) > 1000
)

SELECT
  *
FROM
  result
WHERE
  mae_change_pct > 5.0
ORDER BY mae_change_pct DESC
