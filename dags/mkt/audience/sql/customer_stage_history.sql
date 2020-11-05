INSERT INTO {{ audience_schema }}.{{ brand_code }}_customer_stage_history (
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
    random_num,
    valid_at
)
SELECT
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
    random_num,
    SYSDATE as valid_at
FROM {{ audience_schema }}.{{ brand_code }}_customer_stage
;


SET analyze_threshold_percent TO 0;
ANALYZE {{ audience_schema }}.{{ brand_code }}_customer_stage_history
PREDICATE COLUMNS
;
