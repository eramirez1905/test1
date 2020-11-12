
-- 4th step:
-- Check for differences
TRUNCATE TABLE {{ audience_schema }}.{{ brand_code }}_mparticle_pre_push_diff;
INSERT INTO {{ audience_schema }}.{{ brand_code }}_mparticle_pre_push_diff (
    source_id,
    customer_id,
    channel_control_group,
    channel,
    device_id,
    device_type,
    audience
)
SELECT
    source_id,

    CASE
        WHEN dwh_company_id IN (45) THEN country_iso || '_' || customer_id
        ELSE customer_id
    END AS customer_id,

    COALESCE(main.channel_control_group, 'None'),
    channel,
    -- TODO: We currently only support device_id changes.
    -- Device_id deletions will require more work on mParticle side.
    COALESCE(main.device_id, snap.device_id) AS device_id,
    COALESCE(main.device_type, snap.device_type) AS device_type,
    -- TODO: probably we will need to add LISTAGG when dealing with multiple
    -- audiences for a customer
    COALESCE(main.audience, 'Exclude') AS audience
FROM {{ audience_schema }}.{{ brand_code }}_audience_mparticle_feed_main AS main
JOIN dwh_il.dim_countries AS dc
    USING(source_id)
FULL OUTER JOIN {{ audience_schema }}.{{ brand_code }}_audience_mparticle_feed_snapshot AS snap
    USING(source_id, customer_id, channel)
WHERE
    (
        mkt_common.f_is_distinct_from(main.audience, snap.audience)
        OR mkt_common.f_is_distinct_from(main.channel_control_group, snap.channel_control_group)
        -- Handle device_id changes
        OR mkt_common.f_is_distinct_from(main.device_id, snap.device_id)
        OR mkt_common.f_is_distinct_from(main.device_type, snap.device_type)
    )
    -- Maybe a different solution, just for you THINK ABOUT IT! :)
    -- Because redshift doesn't have `IS DISTINCT FROM` there is this hack:
    -- source: https://stackoverflow.com/questions/10416789/how-to-rewrite-is-distinct-from-and-is-not-distinct-from
    -- ((main.audience != snap.audience OR main.audience IS NULL OR snap.audience IS NULL) AND NOT (main.audience IS NULL AND snap.audience IS NULL))
{% if mparticle.push_sample_data -%}
LIMIT 10
{% endif -%}
;

ANALYZE {{ audience_schema }}.{{ brand_code }}_mparticle_pre_push_diff;
