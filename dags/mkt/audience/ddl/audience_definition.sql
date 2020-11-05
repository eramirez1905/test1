CREATE TABLE IF NOT EXISTS {{ audience_schema }}.{{ brand_code }}_audience_definition (
    source_id SMALLINT NOT NULL,
    source_code VARCHAR(16) NOT NULL,
    -- mandatory Audience matching definitions
    stage VARCHAR(256) NOT NULL,
    variant VARCHAR(256) NOT NULL,
    days_in_stage_enter SMALLINT NOT NULL,
    days_in_stage_exit SMALLINT NOT NULL,
    -- optional (sub-) Audience matching definitions
    loyalty_status_all_verts VARCHAR(32),
    total_non_restaurant_orders INTEGER,
    -- channel + campaign output
    channel VARCHAR(32) NOT NULL,
    campaign VARCHAR(128) NOT NULL,
    -- Audience definition validity
    valid_from DATE,
    valid_until DATE
);
