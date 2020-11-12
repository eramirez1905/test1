-- Main table, available for checking diffs and daily status
CREATE TABLE IF NOT EXISTS {{ audience_schema }}.{{ brand_code }}_audience_braze_feed_main (
    braze_external_id VARCHAR(256) NOT NULL,
    source_id SMALLINT NOT NULL,
    analytical_customer_id VARCHAR(256) NOT NULL,
    master_id VARCHAR(256) NOT NULL,

    channel VARCHAR(32) NOT NULL,
    audience VARCHAR(256) NOT NULL,
    channel_control_group VARCHAR(32)
)
DISTSTYLE KEY
DISTKEY ( analytical_customer_id )
;


CREATE TABLE IF NOT EXISTS {{ audience_schema }}.{{ brand_code }}_audience_braze_feed_snapshot (
    LIKE {{ audience_schema }}.{{ brand_code }}_audience_braze_feed_main
)
;


-- Historical table
CREATE TABLE IF NOT EXISTS {{ audience_schema }}.{{ brand_code }}_audience_braze_feed_history (
    braze_external_id VARCHAR(256) NOT NULL,
    source_id SMALLINT NOT NULL,
    analytical_customer_id VARCHAR(256) NOT NULL,
    master_id VARCHAR(256) NOT NULL,

    channel VARCHAR(32) NOT NULL,
    audience VARCHAR(256) NOT NULL,
    channel_control_group VARCHAR(32),

    valid_at TIMESTAMP DEFAULT SYSDATE
)
DISTSTYLE KEY
DISTKEY ( analytical_customer_id )
;


-- Diff table that will store changes we need to push to Braze
CREATE TABLE IF NOT EXISTS {{ audience_schema }}.{{ brand_code }}_braze_pre_push_diff (
    braze_external_id VARCHAR(256) NOT NULL,
    source_id SMALLINT NOT NULL,
    channel VARCHAR(32) NOT NULL,
    audience VARCHAR(256) NOT NULL,
    channel_control_group VARCHAR(32)
)
DISTSTYLE KEY
DISTKEY ( braze_external_id )
;


-- Diff table in JSONL that will be used to push data to Braze
CREATE TABLE IF NOT EXISTS {{ audience_schema }}.{{ brand_code }}_braze_pre_push_diff_jsonl (
    jsonl VARCHAR(256) NOT NULL
)
;

