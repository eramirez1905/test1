CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.stacked_deliveries`
PARTITION BY created_date
CLUSTER BY country_code, delivery_id, order_id AS
WITH dataset AS (
  SELECT d.country_code AS country_code
    , d.id AS delivery_id
    , d.order_id AS order_id
    , courier_picked_up_at
    , courier_dropped_off_at
    , courier_id
    , o.featured_business_id AS vendor_id
    , CAST(d.created_at AS DATE) AS created_date
    , d.created_at
    , ARRAY_AGG(STRUCT(d.id AS delivery_id
        , d.order_id
        , d.courier_picked_up_at
        , d.courier_dropped_off_at
        , courier_id
        , o.featured_business_id AS vendor_id
        )
      ) OVER (PARTITION BY d.country_code, courier_id ORDER BY d.courier_picked_up_at ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) AS past_deliveries
   , ARRAY_AGG(STRUCT(d.id AS delivery_id
        , d.order_id
        , d.courier_picked_up_at
        , d.courier_dropped_off_at
        , courier_id
        , o.featured_business_id AS vendor_id
        )
      ) OVER (PARTITION BY d.country_code, courier_id ORDER BY d.courier_picked_up_at ROWS BETWEEN CURRENT ROW AND 20 FOLLOWING) AS future_deliveries
  FROM `{{ params.project_id }}.dl.hurrier_deliveries` d
  LEFT JOIN `{{ params.project_id }}.dl.hurrier_orders` o ON d.country_code = o.country_code
    AND d.order_id = o.id
  WHERE courier_picked_up_at IS NOT NULL
    AND courier_dropped_off_at IS NOT NULL
), aggregations AS (
  SELECT * EXCEPT(past_deliveries, future_deliveries)
    , ARRAY(SELECT AS STRUCT *
            FROM UNNEST(past_deliveries) past
            WHERE current_delivery.courier_picked_up_at < past.courier_dropped_off_at
    ) AS deliveries_in_bag_past
  , ARRAY(SELECT AS STRUCT *
            FROM UNNEST(future_deliveries) future
            WHERE future.courier_picked_up_at BETWEEN current_delivery.courier_picked_up_at AND current_delivery.courier_dropped_off_at
    ) AS deliveries_in_bag_future
 FROM dataset current_delivery
), final AS (
  SELECT a.country_code
    , a.created_date
    , a.order_id
    , a.delivery_id
    , a.vendor_id
    , courier_picked_up_at
    , courier_dropped_off_at
    , ARRAY((SELECT AS STRUCT vendor_id, COUNT(vendor_id) AS vendor_count FROM UNNEST (deliveries_in_bag_past) GROUP BY vendor_id)) AS delivery_per_vendor
    , deliveries_in_bag_past
    , deliveries_in_bag_future
  FROM aggregations a
)
SELECT f.* EXCEPT(delivery_per_vendor, deliveries_in_bag_past, deliveries_in_bag_future)
  , ARRAY_LENGTH(deliveries_in_bag_past) AS deliveries_in_bag_past
  , ARRAY_LENGTH(deliveries_in_bag_future) AS deliveries_in_bag_future
  , (SELECT COUNT(s.vendor_id) > 1 FROM UNNEST(deliveries_in_bag_past) s WHERE f.vendor_id = s.vendor_id) is_in_bag_intravendor_past
  , (SELECT COUNT(s.vendor_id) > 1 FROM UNNEST(deliveries_in_bag_future) s WHERE f.vendor_id = s.vendor_id) is_in_bag_intravendor_future
  , deliveries_in_bag_past AS _deliveries_in_bag_past
  , deliveries_in_bag_future AS _deliveries_in_bag_future
FROM final f
