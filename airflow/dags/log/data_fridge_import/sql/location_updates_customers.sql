WITH dataset AS (
  SELECT * EXCEPT(_row_number)
  FROM(
    SELECT *
      , ROW_NUMBER() OVER (PARTITION BY entity_id, customer_address_id, execution_date ORDER BY created_at DESC) AS _row_number
    FROM `ds_model_outputs.location_updates_customers`
    WHERE execution_date = "{{ next_ds }}"
  )
  WHERE _row_number = 1
)
SELECT format_timestamp('%FT%H:%M:%E3SZ',created_at) AS timestamp
  , entity_id AS global_entity_id
  , STRUCT(customer_address_id) AS customer
  , STRUCT(lat AS latitude, lon AS longitude) AS gps
  , location_change_meters
FROM dataset
