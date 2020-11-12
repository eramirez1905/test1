-- populate customer device mapping history table
INSERT INTO {{ audience_schema }}.{{ brand_code }}_customer_device_mapping_history (
    source_id,
    source_code,
    customer_id,
    order_id,
    order_date,
    device_id,
    device_type,
    accounts_per_device,
    valid_at
)
SELECT
    source_id,
    source_code,
    customer_id,
    order_id,
    order_date,
    device_id,
    device_type,
    accounts_per_device,
    SYSDATE as valid_at
FROM {{ audience_schema }}.{{ brand_code }}_customer_device_mapping
;


SET analyze_threshold_percent TO 0;
ANALYZE {{ audience_schema }}.{{ brand_code }}_customer_device_mapping_history
PREDICATE COLUMNS;
