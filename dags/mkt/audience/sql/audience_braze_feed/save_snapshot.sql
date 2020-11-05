-- 6th step:
-- Daily snapshot table will contain previous day data to calculate diff table
TRUNCATE TABLE {{ audience_schema }}.{{ brand_code }}_audience_braze_feed_snapshot;
INSERT INTO {{ audience_schema }}.{{ brand_code }}_audience_braze_feed_snapshot (
    braze_external_id,
    source_id,
    analytical_customer_id,
    master_id,
    channel,
    audience,
    channel_control_group
)
SELECT
    braze_external_id,
    source_id,
    analytical_customer_id,
    master_id,
    channel,
    audience,
    channel_control_group
FROM {{ audience_schema }}.{{ brand_code }}_audience_braze_feed_main
;

ANALYZE {{ audience_schema }}.{{ brand_code }}_audience_braze_feed_snapshot;
