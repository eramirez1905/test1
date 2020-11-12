CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._rps_orders_client_events`
PARTITION BY created_date
CLUSTER BY region, order_id AS
WITH rps_vendor_client_events_dataset AS (
  SELECT *
  FROM `{{ params.project_id }}.cl.rps_vendor_client_events`
), rps_vendor_client_events AS (
  SELECT * EXCEPT(_row_number)
  FROM (
    SELECT orders.id AS order_id
      , region
      , created_date
      , client.name AS client_name
      , client.version
      , client.wrapper_type
      , client.wrapper_version
      , device.id AS device_id
      , device.os_name
      , device.os_version
      , ROW_NUMBER() OVER (PARTITION BY orders.id ORDER BY created_at) AS _row_number
    FROM rps_vendor_client_events_dataset
    WHERE orders.id IS NOT NULL
  )
  WHERE _row_number = 1
)
SELECT *
FROM rps_vendor_client_events
