-- populate customer audience history table
DROP TABLE IF EXISTS customer_stage_variant;
CREATE TEMPORARY TABLE customer_stage_variant
    DISTSTYLE KEY
    DISTKEY ( master_id )
AS
SELECT
    cs.source_id,
    cs.source_code,
    cs.analytical_customer_id,
    cs.master_id,
    cs.customer_id,
    cs.stage,
    cs.stage_age,
    cs.random_num,
    avs.variant
FROM
    {{ audience_schema }}.{{ brand_code }}_customer_stage AS cs
    LEFT JOIN {{ audience_schema }}.{{ brand_code }}_audience_variant_split AS avs
      ON
        cs.source_id = avs.source_id AND cs.stage = avs.stage
        AND (avs.split_from <= cs.random_num AND cs.random_num < avs.split_to)
        AND avs.valid_until IS NULL
;


INSERT INTO {{ audience_schema }}.{{ brand_code }}_customer_audience_history (
    source_id,
    first_order_id,
    analytical_customer_id,
    master_id,
    customer_id,
    stage,
    random_num,
    channel,
    audience,
    valid_at
)
SELECT
    ca.source_id,
    split_part(ca.master_id, '_', 2) as first_order_id,
    ca.analytical_customer_id,
    ca.master_id,
    ca.customer_id,
    csv.stage,
    csv.random_num,
    ca.channel,
    ca.audience,
    SYSDATE AS valid_at
FROM {{ audience_schema }}.{{ brand_code }}_customer_audience AS ca
LEFT JOIN customer_stage_variant AS csv
    USING (master_id)
;


SET analyze_threshold_percent TO 0;
ANALYZE {{ audience_schema }}.{{ brand_code }}_customer_audience_history
PREDICATE COLUMNS;
