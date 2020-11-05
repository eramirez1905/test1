CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._dps_sessions_mapped_to_orders`
PARTITION BY created_date AS
WITH log_orders_data AS (
  SELECT DISTINCT o.country_code
    , c.country_name
    , o.city_id
    , ci.name AS city_name
    , zone_id
    , z.name AS zone_name
    , entity.id AS entity_id
    , vendor.vertical_type AS vertical_type
    , platform_order_code
    , order_id
    , COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone)) AS order_report_date
    , o.created_date
    , o.created_at
    , o.timezone
    , order_placed_at
    , o.vendor.vendor_code  AS vendor_code
    , ROUND(o.timings.to_customer_time / 60, 2) AS to_customer_time
    , ROUND(o.timings.to_vendor_time / 60, 2) AS to_vendor_time
    , ROUND(d.delivery_distance / 1000, 2) AS delivery_distance
    , ROUND(d.delivery_distance, 3) AS delivery_distance_m
    , ROUND(IF(is_preorder IS FALSE AND d.delivery_status = 'completed', o.timings.actual_delivery_time / 60, NULL), 2) AS actual_DT
    , ROUND((o.timings.order_delay / 60), 2) AS order_delay_mins
    , ST_DISTANCE(customer.location, vendor.location) / 1000 AS travel_time_distance_km
  FROM `{{ params.project_id }}.cl.orders` o
  LEFT JOIN UNNEST (deliveries) d ON d.is_primary IS TRUE
  LEFT JOIN `{{ params.project_id }}.cl.countries` c ON o.country_code = c.country_code
  LEFT JOIN UNNEST (c.cities) ci ON o.city_id = ci.id
  LEFT JOIN UNNEST (zones) z ON o.zone_id = z.id
  -- DPS session data starts from 2020-06-30. Considering data from 06/25 for any pre-order data that may exist.
  WHERE o.created_date >= '2020-06-25'
    AND COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone)) >= '2020-06-30'
), dps_data AS (
  SELECT date AS created_date
    , LOWER(country_iso) AS country_code
    , platform AS operating_system
    , order_code_google AS ga_order_code
    , vendor_id
    , vendor_code
    , zone AS dps_test_zone
    , zone_name
    , session_id
    , COALESCE(variant_transaction, variant_last, variant_first) AS variant
    , ab_test_id
    , ab_test_variant
    , IF(dps_sessionid_transaction = 'NA' OR dps_sessionid_transaction = '', NULL, dps_sessionid_transaction) AS dps_sessionid_transaction
    , IF(dps_sessionid_first = 'NA' OR dps_sessionid_first = '', NULL, dps_sessionid_first) AS dps_sessionid_first
    , IF(dps_sessionid_last = 'NA'OR dps_sessionid_last = '', NULL, dps_sessionid_last) AS dps_sessionid_last
    , gmv_eur
    , gfv_eur
    , gmv_local
    , gfv_local
    , delivery_fee_eur
    , delivery_fee_eur_accounting
    , delivery_fee_local
    , delivery_fee_local_accounting
    , COALESCE(dps_delivery_fee_transaction, dps_delivery_fee_last, dps_delivery_fee_first) AS dps_delivery_fee
    , COALESCE(dps_surge_fee_transaction, dps_surge_fee_last, dps_surge_fee_first) AS dps_surge_fee
    , COALESCE(dps_travel_time_transaction, dps_travel_time_last, dps_travel_time_first) AS dps_travel_time
    , COALESCE(dps_sessionid_transaction_created_at, dps_sessionid_last_created_at, dps_sessionid_first_created_at) AS dps_sessionid_created_at
    , COALESCE(tag_transaction, tag_first, tag_last) AS dps_customer_tag
    , COALESCE(dps_minimum_order_value_transaction, dps_minimum_order_value_last, dps_minimum_order_value_first) AS dps_minimum_order_value
  FROM `{{ params.project_id }}.dl.digital_analytics_dps_sessions_mapped_to_orders`
), travel_time_multiplier AS (
  SELECT global_entity_id
  , SAFE_CAST(REGEXP_REPLACE(formula, '[^0-9.]', '') AS FLOAT64) AS formula
  FROM `{{ params.project_id }}.dl.dynamic_pricing_travel_time_formula`
), dps_log_data_merge AS (
  SELECT o.country_code
    , o.country_name
    , o.city_id
    , o.city_name
    , o.zone_id
    , o.zone_name
    , o.entity_id
    , o.vertical_type
    , d.operating_system
    , o.order_report_date
    , o.created_date
    , o.created_at
    , o.timezone
    , d.vendor_id
    , d.vendor_code
    , o.platform_order_code
    , d.ga_order_code AS platform_order_code_ga
    , o.order_id
    , d.dps_test_zone
    , d.variant
    , d.ab_test_id
    , d.ab_test_variant
    , d.session_id
    , COALESCE(d.dps_sessionid_transaction, d.dps_sessionid_last, d.dps_sessionid_first) AS dps_sessionid
    , d.dps_sessionid_created_at
    , d.dps_delivery_fee
    , d.dps_surge_fee
    , d.dps_travel_time
    , d.gmv_eur
    , d.gfv_eur
    , d.gmv_local
    , d.gfv_local
    , d.delivery_fee_eur
    , d.delivery_fee_eur_accounting
    , d.delivery_fee_local
    , d.delivery_fee_local_accounting
    , o.travel_time_distance_km
    , (SELECT stats.estimated_courier_delay FROM f.stats ORDER BY created_at DESC LIMIT 1) AS estimated_courier_delay
    , (SELECT stats.mean_delay FROM f.stats ORDER BY created_at DESC LIMIT 1) AS mean_delay
    , ROUND(o.travel_time_distance_km * tt.formula, 2) AS travel_time
    , d.dps_customer_tag
    , d.dps_minimum_order_value
    , to_customer_time
    , to_vendor_time
    , delivery_distance
    , delivery_distance_m
    , actual_DT AS actual_delivery_time
    , order_delay_mins
  FROM log_orders_data o
  LEFT JOIN dps_data d ON o.country_code = d.country_code
    AND o.platform_order_code = d.ga_order_code
  LEFT JOIN `{{ params.project_id }}.cl.zone_stats` f ON o.country_code = f.country_code
    AND o.zone_id = f.zone_id
    AND TIMESTAMP_TRUNC(dps_sessionid_created_at, MINUTE) = f.created_at_bucket
    -- DPS session data starts from 2020-06-30.
    AND f.created_date >= '2020-06-30'
  LEFT JOIN travel_time_multiplier tt ON tt.global_entity_id = o.entity_id
)
SELECT country_code
  , country_name
  , city_id
  , city_name
  , zone_id
  , zone_name
  , entity_id
  , vertical_type
  , operating_system
  , order_report_date
  , created_date
  , created_at
  , timezone
  , vendor_id
  , vendor_code
  , platform_order_code
  , platform_order_code_ga
  , order_id
  , dps_test_zone
  , variant
  , ab_test_id AS arrow_test_id
  , ab_test_variant AS arrow_test_variant
  , dps_customer_tag
  , session_id
  , dps_sessionid
  , dps_sessionid_created_at
  , dps_delivery_fee
  , dps_surge_fee
  , dps_minimum_order_value
  , dps_travel_time
  , gmv_eur
  , gfv_eur
  , gmv_local
  , gfv_local
  , delivery_fee_eur
  , delivery_fee_eur_accounting
  , delivery_fee_local
  , delivery_fee_local_accounting
  , travel_time_distance_km
  , estimated_courier_delay
  , mean_delay
  , travel_time
  , to_customer_time
  , to_vendor_time
  , delivery_distance
  , delivery_distance_m
  , actual_delivery_time
  , order_delay_mins
FROM dps_log_data_merge
