CREATE TABLE IF NOT EXISTS {{ audience_schema }}.{{ brand_code }}_customer_stage (
    source_id SMALLINT NOT NULL,
    source_code VARCHAR(16) NOT NULL,
    analytical_customer_id VARCHAR(256),
    master_id VARCHAR(256) UNIQUE,
    customer_id VARCHAR(128) NOT NULL,
    last_order_ts TIMESTAMP,
    loyalty_status_all_verts VARCHAR(256),
    total_orders INTEGER,
    total_non_restaurant_orders INTEGER,
    stage VARCHAR(256) NOT NULL,
    stage_age INTEGER,
    random_num DOUBLE PRECISION
)
DISTSTYLE KEY
DISTKEY ( master_id )
;
