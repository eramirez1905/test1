CREATE TABLE IF NOT EXISTS {{ audience_schema }}.{{ brand_code }}_customer_device_mapping (
    source_id SMALLINT NOT NULL,
    source_code VARCHAR(16) NOT NULL,
    customer_id VARCHAR(128) NOT NULL,
    order_id VARCHAR(38) NOT NULL,
    order_date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    device_id VARCHAR(36) NOT NULL,
    device_type VARCHAR(10) NOT NULL,
    accounts_per_device INTEGER NOT NULL
)
DISTSTYLE KEY
DISTKEY ( customer_id )
;
