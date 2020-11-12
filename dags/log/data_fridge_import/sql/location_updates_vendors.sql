WITH vendor_stream AS (
  SELECT * EXCEPT(_row_number)
  FROM (
    SELECT content.vendor_id
      , global_entity_id
      , content.global_vendor_id
      , ROW_NUMBER() OVER(PARTITION BY content.vendor_id, content.global_entity_id ORDER BY timestamp DESC) AS _row_number
    FROM `dl.data_fridge_vendor_stream`)
  WHERE _row_number = 1
)
SELECT format_timestamp('%FT%H:%M:%E3SZ',created_at) AS timestamp
  , entity_id AS global_entity_id
  , STRUCT(vendor_code AS vendor_id, s.global_vendor_id) AS vendor
  , STRUCT(lat AS latitude, lon AS longitude) AS gps
  , location_change_meters
FROM `ds_model_outputs.location_updates_vendors` v
LEFT JOIN vendor_stream s ON v.entity_id = s.global_entity_id
  AND v.vendor_code = s.vendor_id
WHERE execution_date = "{{ next_ds }}"
ORDER BY 1,2
