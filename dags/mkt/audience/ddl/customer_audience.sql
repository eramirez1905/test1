CREATE TABLE IF NOT EXISTS {{ audience_schema }}.{{ brand_code }}_customer_audience (
    source_id SMALLINT NOT NULL,
    source_code VARCHAR(16) NOT NULL,
    analytical_customer_id VARCHAR(256) NOT NULL UNIQUE,
    master_id VARCHAR(256) NOT NULL UNIQUE,
    customer_id VARCHAR(128) NOT NULL,
    channel VARCHAR(16) NOT NULL,
    audience VARCHAR(256) NOT NULL
)
DISTSTYLE KEY
DISTKEY ( master_id )
;
