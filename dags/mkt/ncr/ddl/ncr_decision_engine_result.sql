CREATE TABLE IF NOT EXISTS {{ ncr_schema }}.{{ brand_code }}_decision_engine_result (
  midas_booking_id INTEGER,
  restaurant_id VARCHAR(20),
  global_entity_id VARCHAR(10),
  source_id INTEGER,
  campaign_type VARCHAR(60),
  left_notifications INTEGER,
  max_notifications_to_send INTEGER,
  customer_id VARCHAR(64),
  campaigns_eligible_for BIGINT,
  campaign_targetable_customers BIGINT,
  decision_factor DOUBLE PRECISION,
  factor_rank BIGINT,
  random_top_n BIGINT
)
;
