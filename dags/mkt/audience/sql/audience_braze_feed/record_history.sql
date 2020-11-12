INSERT INTO {{ audience_schema }}.{{ brand_code }}_audience_braze_feed_history (
    braze_external_id,
    source_id,
    analytical_customer_id,
    master_id,
    channel,
    audience,
    channel_control_group,
    valid_at
)
SELECT
    braze_external_id,
    source_id,
    analytical_customer_id,
    master_id,
    channel,
    audience,
    channel_control_group,
    SYSDATE as valid_at
FROM {{ audience_schema }}.{{ brand_code }}_audience_braze_feed_main
;

ANALYZE {{ audience_schema }}.{{ brand_code }}_audience_braze_feed_history;
