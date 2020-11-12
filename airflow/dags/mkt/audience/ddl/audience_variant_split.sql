CREATE TABLE IF NOT EXISTS {{ audience_schema }}.{{ brand_code }}_audience_variant_split (
    source_id SMALLINT NOT NULL,
    source_code VARCHAR(16) NOT NULL,
    -- mandatory split matching definitions
    stage VARCHAR(256) NOT NULL,
    variant VARCHAR(256) NOT NULL,
    -- split output
    split_from DOUBLE PRECISION NOT NULL,
    split_to DOUBLE PRECISION NOT NULL,
    -- split definition validity
    valid_from DATE,
    valid_until DATE
);
