
-- 3rd step:
-- Insert historical data
INSERT INTO {{ audience_schema }}.{{ brand_code }}_audience_mparticle_feed_history (
    source_id,
    master_id,
    customer_id,
    channel_control_group,
    channel,
    audience,
    device_id,
    device_type,
    valid_at
)
SELECT
    source_id,
    master_id,

    CASE
        WHEN dwh_company_id IN (45) THEN country_iso || '_' || customer_id
        ELSE customer_id
    END AS customer_id,

    channel_control_group,
    channel,
    audience,
    device_id,
    device_type,
    SYSDATE as valid_at
FROM {{ audience_schema }}.{{ brand_code }}_audience_mparticle_feed_main
JOIN dwh_il.dim_countries
    USING(source_id)
;

ANALYZE {{ audience_schema }}.{{ brand_code }}_audience_mparticle_feed_history;
