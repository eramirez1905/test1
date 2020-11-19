-- Computing jsonl, this will be pushed to braze.
INSERT INTO {{ ncr_schema }}.{{ brand_code }}_braze_push
SELECT
    '{"external_id": "' || braze_external_id || '", ' ||
    '"ncr_package_type": "' || ncr_package_type || '", ' ||
    '"ncr_publish_date": "' || ncr_publish_date || '", ' ||
    '"ncr_vendor_id": "' || ncr_vendor_id || '"}' AS attributes_jsonl
FROM {{ ncr_schema }}.{{ brand_code }}_braze_main
{% if not environment == "production" -%}
LIMIT 100
{% endif -%}
;
