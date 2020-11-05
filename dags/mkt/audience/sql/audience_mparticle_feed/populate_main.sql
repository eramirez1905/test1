
-- 1st step:
-- Temporary table to use as a filter for users that already belong to a control group and
-- will not be pushed to mParticle
DROP TABLE IF EXISTS current_channel_control_group;
CREATE TEMPORARY TABLE current_channel_control_group
    DISTSTYLE KEY
    DISTKEY ( master_id )
AS
SELECT
    source_id,
    master_id,
    CASE
        WHEN SYSDATE <= new_until THEN new_exclude_from
        ELSE exclude_from
    END AS exclude_from_resolved
FROM {{ crm_schema }}.channel_control_group
WHERE exclude_from_resolved != 'none'
;


-- 2nd step:
-- Daily main table to map user `audience` with `device` not including control group
-- Temp table to map user `audience` with `device` not including control group
TRUNCATE {{ audience_schema }}.{{ brand_code }}_audience_mparticle_feed_main;
INSERT INTO {{ audience_schema }}.{{ brand_code }}_audience_mparticle_feed_main (
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
    audience.source_id,
    audience.master_id,
    audience.customer_id,
    COALESCE(control.exclude_from_resolved, 'None') AS channel_control_group,
    audience.channel,
    audience.audience,
    device.device_id,
    device.device_type
FROM {{ audience_schema }}.{{ brand_code }}_customer_audience AS audience
JOIN {{ audience_schema }}.{{ brand_code }}_customer_device_mapping AS device
    USING (source_id, customer_id)
LEFT JOIN current_channel_control_group AS control
    USING (source_id, master_id)
WHERE
    LOWER(audience.channel) = 'pm'
    -- MKT-2281
    AND device.accounts_per_device = 1
;


ANALYZE {{ audience_schema }}.{{ brand_code }}_audience_mparticle_feed_main;
