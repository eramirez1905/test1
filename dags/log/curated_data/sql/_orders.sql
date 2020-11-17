CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._orders`
PARTITION BY created_date
CLUSTER BY country_code, order_id AS
WITH hurrier_platforms AS (
  SELECT country_code
    , region
    , entity_id
    , display_name AS entity_display_name
    , brand_id
    , hurrier_platforms
  FROM `{{ params.project_id }}.cl.countries` c
  LEFT JOIN UNNEST(c.platforms) platform
  LEFT JOIN UNNEST(platform.hurrier_platforms) hurrier_platforms
)
SELECT o.country_code
  , hp.region
  , o.id AS order_id
  , o.unn_order_id AS platform_order_id
  , o.confirmation_number AS platform_order_code
  , o.platform
  , o.created_date
  , o.created_at
  , o.order_placed_at
  , da.city_id
  , da.timezone
  , CAST('preorder' IN UNNEST(o.tags) AS BOOL) AS is_preorder
  , o.estimated_prep_duration AS estimated_prep_time
  , o.estimated_prep_buffer
  , o.cod_change_for
  , o.cod_collect_at_dropoff
  , o.cod_pay_at_pickup
  , o.delivery_fee
  , o.dropoff_address_id
  , o.online_tip
  , o.order_received_at AS vendor_accepted_at
  , o.order_sent_at AS sent_to_vendor_at
  , o.original_scheduled_pickup_at
  , o.pickup_address_id
  , o.scheduled_dropoff_at AS promised_delivery_time
  , o.scheduled_pickup_at AS updated_scheduled_pickup_at
  , o.stacking_group
  , o.status AS order_status
  , o.tags
  , o.total AS order_value
  , o.utilization AS capacity
  , o.vendor_order_number
  , o.estimated_walk_in_duration
  , o.estimated_walk_out_duration
  , o.estimated_courier_delay
  , o.estimated_driving_time
  , STRUCT(
      geo_point AS location
    ) AS customer
  , STRUCT(
      hp.entity_id AS id
      , hp.entity_display_name AS display_name
      , hp.brand_id
    ) AS entity
  , o.featured_business_id AS vendor_id
FROM `{{ params.project_id }}.dl.hurrier_orders` o
LEFT JOIN `{{ params.project_id }}.cl._addresses` da ON da.country_code = o.country_code
  AND da.id = o.dropoff_address_id
LEFT JOIN hurrier_platforms hp ON o.country_code = hp.country_code
  AND o.platform = hp.hurrier_platforms
WHERE featured_business_id IS NOT NULL
