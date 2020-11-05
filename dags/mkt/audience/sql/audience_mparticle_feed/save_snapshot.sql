
-- 6th step:
-- Daily snapshot table will contain previous day data to calculate diff table
TRUNCATE TABLE {{ audience_schema }}.{{ brand_code }}_audience_mparticle_feed_snapshot;
INSERT INTO {{ audience_schema }}.{{ brand_code }}_audience_mparticle_feed_snapshot (
    source_id,
    master_id,
    customer_id,
    channel_control_group,
    channel,
    audience,
    device_id,
    device_type
)
SELECT
    source_id,
    master_id,
    customer_id,
    channel_control_group,
    channel,
    audience,
    device_id,
    device_type
FROM {{ audience_schema }}.{{ brand_code }}_audience_mparticle_feed_main
;

ANALYZE {{ audience_schema }}.{{ brand_code }}_audience_mparticle_feed_main;
