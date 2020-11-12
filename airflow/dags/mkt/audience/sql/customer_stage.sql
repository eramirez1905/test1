DROP TABLE IF EXISTS enriched_fct_customer;
-- For cases where one (analytical) customer has multiple customer_id's we push based on the
-- customer_id of last order.
CREATE TEMPORARY TABLE enriched_fct_customer
    DISTSTYLE KEY
    DISTKEY ( master_id )
AS
SELECT
    fc.source_id,
    dc.dwh_source_code AS source_code,
    fc.analytical_customer_id,
    fc.source_id || '_' || fc.first_order_id AS master_id,
    lo.customer_id AS last_customer_id,
    fc.last_order_date AS last_order_ts,
    -- segmentation
    acs.loyalty_status_all_verts,
    -- metrics
    COUNT(CASE WHEN o.is_sent THEN o.order_id END) AS total_orders,
    COUNT(CASE WHEN o.is_sent AND o.is_shopping THEN o.order_id END) AS total_non_restaurant_orders
FROM {{ il_schema }}.fct_customer AS fc
LEFT JOIN dwh_il.dim_countries AS dc
    USING (source_id)
LEFT JOIN {{ crm_schema }}.analytical_customer_segmentation AS acs
    USING (source_id, analytical_customer_id)
-- Get information about the last order
LEFT JOIN {{ il_schema }}.ranked_fct_order AS lo
    ON fc.source_id = lo.source_id
    AND fc.last_order_id = lo.order_id
-- Get information about all orders
LEFT JOIN {{ il_schema }}.ranked_fct_order AS o
    ON fc.source_id = o.source_id
    AND fc.analytical_customer_id = o.analytical_customer_id
WHERE
    dc.is_active IS TRUE
    AND fc.source_id IN {{ source_id | source_id_filter }}
GROUP BY 1, 2, 3, 4, 5, 6, 7
;

TRUNCATE {{ audience_schema }}.{{ brand_code }}_customer_stage;
INSERT INTO {{ audience_schema }}.{{ brand_code }}_customer_stage (
    source_id,
    source_code,
    analytical_customer_id,
    master_id,
    customer_id,
    last_order_ts,
    loyalty_status_all_verts,
    total_orders,
    total_non_restaurant_orders,
    stage,
    stage_age,
    random_num
)
SELECT
    source_id,
    source_code,
    analytical_customer_id,
    master_id,
    last_customer_id AS customer_id,
    last_order_ts,
    loyalty_status_all_verts,
    total_orders,
    total_non_restaurant_orders,
    {{ audience_schema }}.f_customer_stage(
        CURRENT_DATE, total_orders, last_order_ts::DATE
    ) AS stage,
    {{ audience_schema }}.f_customer_stage_age(
        CURRENT_DATE, total_orders, last_order_ts::DATE
    ) AS stage_age,
    -- If we have assigned a customer to a variation we don't want it to change anymore, as long
    -- as the users does not change their stage. However, upon stage changes we want to recompute
    -- random_num to have unbiased variant attribution.
    -- Feeding master_id + '_' + stage into MD5 hashes fulfills this purpose.
    1.0 * STRTOL(LEFT(MD5(master_id + '_' + stage), 4), 16) / 65536 AS random_num
FROM enriched_fct_customer AS fc
;


SET analyze_threshold_percent TO 0;
ANALYZE {{ audience_schema }}.{{ brand_code }}_customer_stage
PREDICATE COLUMNS
;
