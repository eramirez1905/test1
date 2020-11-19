CREATE TEMPORARY FUNCTION delivery_type(delivery_provider STRING)
RETURNS STRING AS(
  CASE
    WHEN LOWER(delivery_provider) LIKE '%vendor%' THEN 'vendor delivery'
    WHEN LOWER(delivery_provider) LIKE '%vd%' THEN 'vendor delivery'
    WHEN LOWER(delivery_provider) LIKE '%platform%' THEN 'own delivery'
    WHEN LOWER(delivery_provider) LIKE '%od%' THEN 'own delivery'
    ELSE LOWER(delivery_provider)
  END
);

CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._pricing_sessions_details`
PARTITION BY created_date AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 30 DAY) AS start_date
    , DATE_SUB('{{ next_ds }}', INTERVAL 6 WEEK) AS start_date_pricing
), pricing_data AS (
  SELECT brand AS platform
    , global_entity_id AS entity_id
    -- Cookie user identifier
    , fullvisitor_id AS full_visitor_id
    -- Epoch Timestamp
    , visit_id
    , DATE(TIMESTAMP_SECONDS(visit_id)) AS created_date
    , TIMESTAMP_SECONDS(visit_id) AS visit_start_time
    , uuid AS unique_user_id
    , country AS country_name
    , platform AS device_type
    , visit_type
    , source
    , medium
    -- Concatenated platform_order_code if more than one is placed per session
    , ecommerce_transaction_id
    , shop_dims.hit_number
    , shop_dims.action AS event_type
    , shop_dims.shop_code AS vendor_code
    , LOWER(IF(LOWER(shop_dims.expedition_type) LIKE ('%pickup%'), 'pickup', shop_dims.expedition_type)) AS expedition_type
    , delivery_type(shop_dims.delivery_provider) AS delivery_type
    , shop_dims.vertical_type
    , shop_dims.minimum_order_value
    , shop_dims.delivery_fee
    , shop_dims.surcharge_value
    , shop_dims.total_value AS order_value
    , shop_dims.transaction_id AS platform_order_code
  FROM `{{ params.project_id }}.dl.digital_analytics_pricing_sessions_details` ps
  LEFT JOIN UNNEST (shop_dims) shop_dims
  -- Query takes too long to execute
  WHERE partitionDate > (SELECT start_date_pricing FROM parameters)
), orders_data AS (
  -- Orders data taken to remove non-operational entities from pricing data.
  SELECT DISTINCT entity.id AS entity_id
  FROM `{{ params.project_id }}.cl.orders` o
  WHERE created_date >= (SELECT start_date FROM parameters)
)
SELECT pd.created_date
  , pd.country_name
  , pd.entity_id
  , pd.platform
  , pd.full_visitor_id
  , pd.visit_id
  , pd.visit_start_time
  , pd.unique_user_id
  , pd.device_type
  , pd.visit_type
  , pd.source
  , pd.medium
  , pd.ecommerce_transaction_id
  , ARRAY_AGG(
      STRUCT(pd.hit_number
        , pd.event_type
        , pd.vendor_code
        , pd.expedition_type
        , pd.delivery_type
        , pd.vertical_type
        , pd.minimum_order_value
        , pd.delivery_fee
        , pd.surcharge_value
        , pd.order_value
        , pd.platform_order_code
    )) AS sessions
FROM orders_data o
LEFT JOIN pricing_data pd USING (entity_id)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
