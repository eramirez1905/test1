CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.gps_accuracy_report`
PARTITION BY report_date AS
WITH raw_geo_data AS (
  SELECT o.country_code
    , d.city_id
    , entity.display_name AS entity
    , d.id AS delivery_id
    , vendor.id AS vendor_id
    , vendor.vendor_code
    , v.vendor_name
    , DATE(d.rider_dropped_off_at, d.timezone) AS report_date
    , FORMAT_DATE('%G-%V', DATE(d.rider_dropped_off_at, d.timezone)) AS report_week
    , IF('existing_address' IN UNNEST(o.tags), 'existing_address', 'new_address') AS address_tag
    , 'address_reviewed' IN UNNEST(o.tags) AS is_address_reviewed
    , d.pickup AS pickup_location
    , d.dropoff AS dropoff_location
    , customer.location AS customer_location
    , vendor.location AS vendor_location
    , d.auto_transition.pickup AS is_pickup_auto_transitioned
    , d.auto_transition.dropoff AS is_dropoff_auto_transitioned
  FROM `{{ params.project_id }}.cl.orders` o
  LEFT JOIN UNNEST(deliveries) d
  LEFT JOIN `{{ params.project_id }}.cl.vendors` v ON o.country_code = v.country_code
    AND o.vendor.id = v.vendor_id
  WHERE DATE(d.rider_dropped_off_at, d.timezone) >= DATE_SUB('{{ next_ds }}', INTERVAL 6 WEEK)
    AND d.delivery_status = 'completed'
), distances AS (
  SELECT country_code
    , entity
    , city_id
    , delivery_id
    , vendor_id
    , vendor_code
    , vendor_name
    , report_date
    , report_week
    , address_tag
    , is_address_reviewed
    , is_pickup_auto_transitioned
    , is_dropoff_auto_transitioned
    , CAST(ST_DISTANCE(dropoff_location, customer_location) AS FLOAT64) AS distance_dropoff_to_customer
    , CAST(ST_DISTANCE(pickup_location, vendor_location) AS FLOAT64) AS distance_pickup_to_vendor
  FROM raw_geo_data
)
SELECT d.country_code
  , co.country_name
  , d.entity
  , ci.name AS city_name
  , d.vendor_id
  , d.vendor_code
  , d.vendor_name
  , d.report_date
  , d.report_week
  , d.address_tag
  , d.is_address_reviewed
  , COUNT(DISTINCT d.delivery_id) AS count_deliveries
  , COUNTIF(is_pickup_auto_transitioned) AS near_pickup
  , COUNTIF(is_dropoff_auto_transitioned) AS near_dropoff
  , COUNTIF(d.distance_dropoff_to_customer IS NOT NULL) AS deliveries_with_distance_dropoff_to_customer
  , COUNTIF(d.distance_dropoff_to_customer > 100) AS deliveries_over_100_customer
  , COUNTIF(d.distance_dropoff_to_customer > 200) AS deliveries_over_200_customer
  , COUNTIF(d.distance_dropoff_to_customer > 500) AS deliveries_over_500_customer
  , SUM(d.distance_dropoff_to_customer) AS sum_distance_dropoff_to_customer
  , COUNTIF(d.distance_pickup_to_vendor IS NOT NULL) AS deliveries_with_distance_pickup_to_vendor
  , COUNTIF(d.distance_pickup_to_vendor > 100) AS deliveries_over_100_vendor
  , COUNTIF(d.distance_pickup_to_vendor > 200) AS deliveries_over_200_vendor
  , COUNTIF(d.distance_pickup_to_vendor > 500) AS deliveries_over_500_vendor
  , SUM(d.distance_pickup_to_vendor) AS sum_distance_pickup_to_vendor
FROM distances d
LEFT JOIN `{{ params.project_id }}.cl.countries` co ON d.country_code = co.country_code
LEFT JOIN UNNEST(cities) ci ON d.city_id = ci.id
-- removing darkstore pickers to not impact utr as they do not have orders, only working hours
WHERE d.country_code NOT LIKE '%dp%'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
