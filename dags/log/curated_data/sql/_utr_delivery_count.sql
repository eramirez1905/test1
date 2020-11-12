CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._utr_delivery_count`
PARTITION BY created_date AS
WITH orders_data AS (
  SELECT country_code
    , o.city_id
    , zone_id
    , entity.id AS entity_id
    , order_id
    , created_date
    , DATE(d.rider_dropped_off_at, o.timezone) AS delivery_date
    , o.timezone
    , d.id AS delivery_id
    , vendor.id AS vendor_id
    , d.rider_id AS rider_id
    -- Added to avoid NULL values in stacked_deliveries
    , COALESCE(stacked_deliveries, 0) AS stacked_deliveries
    , s.delivery_id AS stacked_delivery_id
  FROM `{{ params.project_id }}.cl.orders` o
  LEFT JOIN UNNEST(deliveries) d
  LEFT JOIN UNNEST(d.stacked_deliveries_details) s
  WHERE d.delivery_status = 'completed'
), vendor_data AS (
  -- Distinct data taken as the stacked deliveries have been unnested causing
  -- duplicate values for the below select. Distinct taken to avoid duplicate
  -- data in the join of the next (intravendor_data) subquery.
  SELECT DISTINCT created_date
    , country_code
    , delivery_id
    , vendor_id
  FROM orders_data
), intravendor_data AS (
  SELECT o.country_code
    , o.city_id
    , o.zone_id
    , o.entity_id
    , o.order_id
    , o.created_date
    , o.delivery_date
    , o.timezone
    , o.delivery_id
    , o.vendor_id
    , o.rider_id
    , o.stacked_deliveries
    , o.stacked_delivery_id
    , v.vendor_id AS stacked_vendor_id
    , (v.vendor_id = o.vendor_id) AS is_stacked_intravendor
  FROM orders_data o
  -- JOIN done to retrieve the vendor_id of the stacked deliveries
  LEFT JOIN vendor_data v ON o.stacked_delivery_id = v.delivery_id
    AND o.country_code = v.country_code
    AND o.created_date = v.created_date
), intravendor_array_data AS (
  SELECT country_code
    , delivery_date
    , delivery_id
    , is_stacked_intravendor
    , ARRAY_AGG(stacked_delivery_id) AS stacked_delivery_details
  FROM intravendor_data
  WHERE is_stacked_intravendor IS TRUE
  GROUP BY 1, 2, 3, 4
), calculate_intravendor_base_key AS (
  SELECT o.country_code
    , o.city_id
    , o.zone_id
    , o.entity_id
    , o.order_id
    , o.created_date
    , o.delivery_date
    , o.timezone
    , o.delivery_id
    , o.vendor_id
    , o.rider_id
    , o.stacked_deliveries
    , o.stacked_delivery_id
    , o.stacked_vendor_id
    , o.is_stacked_intravendor
    -- array contains the stacked deliveries and the delivery_id itself. Cannot use
    -- COALESCE(o1.stacked_delivery_details, []) as the data returns null for deliveries not stacked.
    , ARRAY_CONCAT(ARRAY(SELECT * FROM o1.stacked_delivery_details), [o.delivery_id]) AS _intravendor_base_key
  FROM intravendor_data o
  LEFT JOIN intravendor_array_data o1 USING (country_code, delivery_date, delivery_id)
), calculate_intravendor_key AS (
  SELECT country_code
    , city_id
    , zone_id
    , entity_id
    , order_id
    , created_date
    , delivery_date
    , timezone
    , delivery_id
    , vendor_id
    , rider_id
    , stacked_deliveries
    , is_stacked_intravendor
    , stacked_delivery_id
    , stacked_vendor_id
    , TO_JSON_STRING(ARRAY(SELECT * FROM o._intravendor_base_key ORDER BY 1)) AS _intravendor_key
  FROM calculate_intravendor_base_key o
), delivery_count_base AS (
  SELECT country_code
    , delivery_date
    , order_id
    , delivery_id
    , COUNTIF(is_stacked_intravendor IS TRUE) AS _count_intravendor_true
    , COUNTIF(is_stacked_intravendor IS FALSE) AS _count_intravendor_false
  FROM intravendor_data
  GROUP BY 1, 2, 3, 4
), delivery_count_data AS (
  SELECT country_code
    , city_id
    , zone_id
    , entity_id
    , order_id
    , created_date
    , delivery_date
    , timezone
    , delivery_id
    , vendor_id
    , rider_id
    , stacked_deliveries
    , is_stacked_intravendor
    , stacked_delivery_id
    , stacked_vendor_id
    , _count_intravendor_true
    , _count_intravendor_false
    -- Case created to count the total deliveries in the stack.
    , IF(is_stacked_intravendor, _count_intravendor_true + 1, 1) AS delivery_count
    , _intravendor_key
  FROM calculate_intravendor_key
  LEFT JOIN delivery_count_base USING (delivery_date, country_code, order_id, delivery_id)
), max_delivery_count AS (
  SELECT country_code
    , delivery_date
    , delivery_id
    , MAX(delivery_count) AS delivery_count
    , _intravendor_key
  FROM delivery_count_data
  GROUP BY 1, 2, 3, 5
)
SELECT d.country_code
  , d1.order_id
  , d1.created_date
  , d.delivery_id
  , d1.stacked_deliveries
  , d1.is_stacked_intravendor
  , d.delivery_count AS delivery_count
  , d._intravendor_key AS intravendor_key
FROM max_delivery_count d
LEFT JOIN delivery_count_data d1 USING (delivery_date, country_code, delivery_id, _intravendor_key)
-- data grouped because `stacked_delivery_id` in delivery_count_data is not considered in the final select and thereby
-- causes duplicates. Some delivery_ids are duplicated but have different `is_stacked_intravendor` and this is caused
-- when a delivery has an intravendor and intervendor delivery stacked with itself.
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
