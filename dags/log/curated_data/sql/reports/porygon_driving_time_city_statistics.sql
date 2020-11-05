CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.porygon_driving_time_city_statistics` AS
WITH dataset AS (
  SELECT o.country_code
    , co.country_name
    , ci.name AS city_name
    , d.city_id
    , DATE(d.rider_dropped_off_at, o.timezone) AS created_date
    , d.id AS delivery_id
    , p.vehicle_profile AS porygon_vehicle_profile
    , d.vehicle.profile AS delivery_vehicle_profile
    , o.vendor.id AS vendor_id
    , o.entity.display_name AS entity_display_name
    , o.vendor.vendor_code AS vendor_code
    , o.vendor.name AS vendor_name
    , o.customer.location AS customer_location
    , p.drive_time_value AS porygon_driving_time
    , ROUND((d.timings.to_customer_time / 60), 0) AS actual_driving_time
  FROM `{{ params.project_id }}.cl.orders` o
  LEFT JOIN UNNEST (deliveries) d
  LEFT JOIN UNNEST(o.porygon) p
  LEFT JOIN `{{ params.project_id }}.cl.countries` co ON o.country_code = co.country_code
  LEFT JOIN UNNEST(co.cities) ci ON o.city_id = ci.id
  WHERE o.created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 2 MONTH)
    AND d.timings.to_customer_time > 0
    AND d.delivery_status = 'completed'
    AND (d.stacked_deliveries < 1)  --- this means stacked is false
    -- removing darkstore pickers to not impact utr as they do not have orders, only working hours
    AND o.country_code NOT LIKE '%dp%'
), aggregations AS (
  SELECT country_code
    , entity_display_name
    , porygon_driving_time
    , country_name
    , city_name
    , porygon_vehicle_profile
    , COUNT(delivery_id) AS deliveries
    , ROUND(AVG(actual_driving_time),0) AS actual_driving_time
    , APPROX_QUANTILES(actual_driving_time, 100)[OFFSET(25)] AS percentile_25
    , APPROX_QUANTILES(actual_driving_time, 100)[OFFSET(50)] AS percentile_50
    , APPROX_QUANTILES(actual_driving_time, 100)[OFFSET(75)] AS percentile_75
    , APPROX_QUANTILES(actual_driving_time, 100)[OFFSET(90)] AS percentile_90
    , ROUND(SQRT(SUM(POW(porygon_driving_time - actual_driving_time, 2)) / COUNT(1)), 2) AS rmse
  FROM dataset
  GROUP BY  1, 2, 3, 4, 5, 6
)
SELECT *
  , (deliveries / (SUM(deliveries) OVER (PARTITION BY country_code, entity_display_name, city_name, porygon_vehicle_profile)) * 100) AS perc_deliveries
FROM aggregations
