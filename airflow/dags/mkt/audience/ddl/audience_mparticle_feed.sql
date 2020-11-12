-- Main table, available for checking diffs and daily status
CREATE TABLE IF NOT EXISTS {{ audience_schema }}.{{ brand_code }}_audience_mparticle_feed_main (
    source_id SMALLINT NOT NULL,
    master_id VARCHAR(256) NOT NULL UNIQUE,
    customer_id VARCHAR(128) NOT NULL,  -- last `customer_id`
    channel_control_group VARCHAR(32),

    channel VARCHAR(32) NOT NULL,
    audience VARCHAR(256) NOT NULL,
    device_id VARCHAR(36) NOT NULL,
    device_type VARCHAR(10) NOT NULL
)
DISTSTYLE KEY
DISTKEY ( customer_id )
;


CREATE TABLE IF NOT EXISTS {{ audience_schema }}.{{ brand_code }}_audience_mparticle_feed_snapshot
(LIKE {{ audience_schema }}.{{ brand_code }}_audience_mparticle_feed_main)
;


-- Historical table
CREATE TABLE IF NOT EXISTS {{ audience_schema }}.{{ brand_code }}_audience_mparticle_feed_history (
    source_id SMALLINT NOT NULL,
    master_id VARCHAR(256) NOT NULL UNIQUE,
    customer_id VARCHAR(128) NOT NULL,  -- last `customer_id`
    channel_control_group VARCHAR(32),

    channel VARCHAR(32) NOT NULL,
    audience VARCHAR(256) NOT NULL,
    device_id VARCHAR(36) NOT NULL,
    device_type VARCHAR(10) NOT NULL,

    valid_at TIMESTAMP DEFAULT SYSDATE
)
DISTSTYLE KEY
DISTKEY ( customer_id )
;


-- Diff table that will be used to push data to mParticle
CREATE TABLE IF NOT EXISTS {{ audience_schema }}.{{ brand_code }}_mparticle_pre_push_diff (
    source_id SMALLINT NOT NULL,
    customer_id VARCHAR(128) NOT NULL,  -- last `customer_id`
    channel_control_group VARCHAR(32),

    channel VARCHAR(32) NOT NULL,
    device_id VARCHAR(36) NOT NULL,
    device_type VARCHAR(10) NOT NULL,
    audience VARCHAR(256) NOT NULL
)
DISTSTYLE KEY
DISTKEY ( customer_id )
;
