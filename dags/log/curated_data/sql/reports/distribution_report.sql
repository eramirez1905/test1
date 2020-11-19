CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.distribution_report`
PARTITION BY report_date AS
SELECT o.country_code
  , co.country_name
  , o.entity.display_name AS entity_display_name
  , d.city_id
  , ci.name AS city_name
  , o.zone_id
  , zz.name AS zone_name
  , o.region
  , CAST(DATETIME(d.created_at, d.timezone) AS DATE) AS report_date
  , FORMAT_DATE('%G-%V', DATE(d.created_at, d.timezone)) AS delivery_week
  , order_id
  , d.id AS delivery_id
  , o.is_preorder AS preorder
  , d.vehicle.profile AS vehicle_type
  , d.timings.rider_accepting_time / 60 AS accepting_time
  , d.timings.rider_reaction_time / 60 AS rider_reaction_time
  , d.timings.to_vendor_time / 60 AS to_vendor
  , d.timings.at_vendor_time / 60 AS at_vendor
  , d.timings.to_customer_time / 60 AS to_customer
  , d.timings.at_customer_time / 60 AS at_customer
  , d.timings.actual_delivery_time / 60 AS delivery_time_eff
  , d.timings.delivery_delay / 60 AS delivery_delay_eff
  , d.timings.dispatching_time / 60 AS dispatching_time
  , d.timings.vendor_late / 60 AS vendor_late
  , d.timings.rider_late / 60 AS courier_late
  , d.timings.assumed_actual_preparation_time / 60 AS assumed_actual_preparation_time
  , IF(is_preorder IS FALSE, o.timings.hold_back_time / 60, NULL) AS hold_back_time
  , o.estimated_prep_time / 60 AS estimated_prep_time
  , d.delivery_distance / 1000 AS delivery_distance
  , d.pickup_distance_google / 1000 AS pickup_distance_tes
  , d.pickup_distance_manhattan / 1000 AS pickup_distance_manhattan
  , d.dropoff_distance_google / 1000 AS dropoff_distance_tes
  , d.dropoff_distance_manhattan / 1000 AS dropoff_distance_manhattan
  , d.pickup_distance_dte / 1000 AS pickup_distance_dte
  , d.dropoff_distance_dte / 1000 AS dropoff_distance_dte
  , COALESCE(stacked_deliveries, 0) AS stacked_deliveries
FROM `{{ params.project_id }}.cl.orders` o
LEFT JOIN UNNEST(deliveries) d
LEFT JOIN `{{ params.project_id }}.cl.countries` AS co ON o.country_code = co.country_code
LEFT JOIN UNNEST(co.cities) ci ON d.city_id = ci.id
LEFT JOIN UNNEST(ci.zones) zz ON o.zone_id = zz.id
-- removing darkstore pickers to not impact utr as they do not have orders, only working hours
WHERE o.country_code NOT LIKE '%dp%'
  AND o.created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 4 WEEK)
  AND d.delivery_status = 'completed'
  AND d.is_redelivery = FALSE
