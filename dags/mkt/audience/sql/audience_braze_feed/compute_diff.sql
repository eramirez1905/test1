-- Check for differences
TRUNCATE TABLE {{ audience_schema }}.{{ brand_code }}_braze_pre_push_diff;
INSERT INTO {{ audience_schema }}.{{ brand_code }}_braze_pre_push_diff
SELECT
    braze_external_id,
    source_id,
    channel,
    COALESCE(main.audience, 'Exclude') AS audience,
    COALESCE(main.channel_control_group, 'None') AS channel_control_group
FROM {{ audience_schema }}.{{ brand_code }}_audience_braze_feed_main AS main
FULL OUTER JOIN {{ audience_schema }}.{{ brand_code }}_audience_braze_feed_snapshot AS snap
    USING(braze_external_id, source_id, channel)
WHERE
    mkt_common.f_is_distinct_from(main.audience, snap.audience)
    OR mkt_common.f_is_distinct_from(main.channel_control_group, snap.channel_control_group)
{% if braze.push_sample_data -%}
LIMIT 10
{% endif -%}
;

ANALYZE {{ audience_schema }}.{{ brand_code }}_braze_pre_push_diff;


TRUNCATE TABLE {{ audience_schema }}.{{ brand_code }}_braze_pre_push_diff_jsonl;
INSERT INTO {{ audience_schema }}.{{ brand_code }}_braze_pre_push_diff_jsonl
SELECT
    {{ audience_schema }}.f_audience_to_jsonl(braze_external_id, audience, channel_control_group)
FROM {{ audience_schema }}.{{ brand_code }}_braze_pre_push_diff
;

ANALYZE {{ audience_schema }}.{{ brand_code }}_braze_pre_push_diff_jsonl;
