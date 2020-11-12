/*
This table will contain FULL history of audience assignments. Every time audience is recomputed,
all the computed rows are inserted into this table. It doesn't make sense to include all the fields
here from the main tables:
- ACID is not here, because it can change and we would have to keep track of it in other way anyway
- customer_id is not here, because we are interested in a person, not in profile and last
  customer_id will be changing too
- random_number is missing because we are storing audience here (random_number has no other value
  than identifying which variant does user belong to)

Current use cases for this table are:
- audience size evolution over time
- comparing orders between variants / stages and their long term effects on orders

What this means in practise:
- this table will get big very quickly since we insert one row per ACID daily
- will be used to aggregate by `audience` column
- will be used to find orders from `ranked_fct_order`
- to get ACID, join to fct_customer using first_order_id and source_id

 */
CREATE TABLE IF NOT EXISTS {{ audience_schema }}.{{ brand_code }}_customer_audience_history (
    -- source_id + first_order_id is the same as master_id and is provided here for easier join with ranked_fct_order
    source_id SMALLINT NOT NULL,
    first_order_id VARCHAR(256) NOT NULL,
    analytical_customer_id VARCHAR(256) NOT NULL UNIQUE,
    master_id VARCHAR(256) NOT NULL UNIQUE,
    customer_id VARCHAR(128) NOT NULL,

    stage VARCHAR(256) NOT NULL,
    random_num DOUBLE PRECISION NOT NULL,
    channel VARCHAR(16) NOT NULL,
    audience VARCHAR(256) NOT NULL,

    valid_at TIMESTAMP DEFAULT SYSDATE
)
DISTSTYLE KEY
DISTKEY ( first_order_id )
;
