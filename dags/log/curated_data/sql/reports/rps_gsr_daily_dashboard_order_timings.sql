CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.rps_gsr_daily_dashboard_order_timings`
PARTITION BY created_date
CLUSTER BY entity_id, region, display_name, vertical_type AS
WITH vendors AS (
  SELECT *
  FROM (
    SELECT v.entity_id
      , v.vendor_code
      , v.vertical_type
      , ROW_NUMBER() OVER (PARTITION BY v.entity_id, v.vendor_code ORDER BY rps.updated_at DESC) AS _row_number
    FROM `{{ params.project_id }}.cl.vendors_v2` v
    CROSS JOIN UNNEST(rps) rps
  )
  WHERE _row_number = 1
), rps_orders_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl.rps_orders`
  WHERE created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 3 MONTH)
    AND timings.transmission_time >= 0
    AND timings.response_time >= 0
), order_timings AS (
  SELECT o.created_date
    , o.region
    , o.entity.id AS entity_id
    , o.entity.display_name
    , o.entity.brand_id
    , o.delivery_type
    , v.vertical_type
    , o.order_id
    , o.service
    , o.client.name AS application_client_name
    , o.timings.transmission_time
    , o.timings.response_time
    , ROW_NUMBER() OVER (sampling_window) AS _row_number_sampling
  FROM rps_orders_dataset o
  LEFT JOIN vendors v ON o.vendor.code = v.vendor_code
    AND o.entity.id = v.entity_id
  -- This window is used for sampling ONLY, to redeuce the number of records
  WINDOW sampling_window AS (
    PARTITION BY o.created_date
      , o.region
      , o.entity.id
      , o.entity.display_name
      , o.entity.brand_id
      , o.delivery_type
      , v.vertical_type
      , o.service
      , o.client.name
    ORDER BY timings.transmission_time, timings.response_time
  )
)
SELECT * EXCEPT(_row_number_sampling)
FROM order_timings
WHERE MOD(_row_number_sampling, 10) = 0
