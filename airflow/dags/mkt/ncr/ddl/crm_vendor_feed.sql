CREATE TABLE IF NOT EXISTS {{ ncr_schema }}.{{ brand_code }}_crm_vendor_feed (
    dwh_source_code VARCHAR(10),
    local_vendor_id VARCHAR(20),
    local_created_at TIMESTAMP,
    local_deal_description VARCHAR(256),
    local_deal_end_date DATE,
    local_deal_name VARCHAR(100),
    local_deal_start_date DATE,
    local_deal_type VARCHAR(20)
)
DISTKEY (local_vendor_id)
;
