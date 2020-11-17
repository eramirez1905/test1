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


TRUNCATE {{ audience_schema }}.{{ brand_code }}_audience_braze_feed_main;
INSERT INTO {{ audience_schema }}.{{ brand_code }}_audience_braze_feed_main (
    braze_external_id,
    source_id,
    analytical_customer_id,
    master_id,
    channel,
    audience,
    channel_control_group
)
SELECT
    ext.external_id AS braze_external_id,
    source_id,
    analytical_customer_id,
    master_id,
    channel,
    audience,
    COALESCE(ctrl.exclude_from_resolved, 'None') AS channel_control_group  -- FIXME: Is this correct?
FROM {{ audience_schema }}.{{ brand_code }}_customer_audience AS ca
NATURAL LEFT JOIN {% if brand_common_name == 'talabat' -%}
  -- To account for Talabat guest users we need to get the BrazeIDs from both Talabat CRM schemas
  (SELECT * FROM mkt_crm_tb.unique_customer_ids UNION ALL SELECT * FROM mkt_crm_tb_guest.unique_customer_ids)
{%- else -%}
  {{ crm_schema }}.unique_customer_ids
{%- endif %} AS ext
NATURAL LEFT JOIN current_channel_control_group AS ctrl
WHERE
    LOWER(channel) = 'crm'
    -- FIXME: This should not happen, but it does ._.
    AND ext.external_id IS NOT NULL
;
