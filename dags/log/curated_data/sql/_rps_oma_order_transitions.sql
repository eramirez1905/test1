CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._rps_oma_order_transitions`
PARTITION BY created_date
CLUSTER BY region, order_id AS
WITH oma_order_events_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.dl.oma_order_events`
), oma_order_events AS (
  SELECT IF(region = 'asia', 'ap', region) AS region
    , created_date
    , created_at
    , IF(oldState = 'UNDEFINED', NULL, oldState) AS old_state
    , newState AS new_state
    , CASE
        WHEN newState IS NOT NULL
          THEN newState
        WHEN notification = 'ARRIVED_AT_VENDOR'
          THEN 'NEAR_VENDOR'
        WHEN notification = 'ARRIVED_AT_CUSTOMER'
          THEN 'NEAR_CUSTOMER'
        WHEN notification = 'DELAYED'
          THEN 'DELAYED'
        WHEN metadata.type = 'Rps.OrderProcessing.RestaurantOwnDelivery.Prepared'
          THEN 'PREPARED'
        ELSE NULL
      END AS state
    , rpsOrderId AS order_id
    , platformOrderId AS order_code
    , globalEntityId AS entity_id
    , platformGlobalKey AS platform_key
    , rpsVendorId AS vendor_id
    , platformVendorId AS vendor_code
    , reason
    , owner
    , stage
    , source
  FROM oma_order_events_dataset
  WHERE (oldState IS NOT NULL
    OR newState IS NOT NULL
    OR notification IS NOT NULL
    OR metadata.type = 'Rps.OrderProcessing.RestaurantOwnDelivery.Prepared'
    )
)
SELECT region
  , order_id
  , CAST(MIN(created_at) AS DATE) AS created_date
  , ARRAY_AGG(
      STRUCT(
        created_at
        , stage
        , state
        , source
        , owner
      ) ORDER BY created_at
    ) AS transitions
FROM oma_order_events
GROUP BY 1,2
