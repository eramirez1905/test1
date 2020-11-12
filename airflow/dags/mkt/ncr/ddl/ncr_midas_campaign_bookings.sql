CREATE TABLE IF NOT EXISTS {{ ncr_schema }}.{{ brand_code }}_ncr_midas_campaign_bookings (
  -- midas id
  midas_booking_id INTEGER,
  -- entity information
  midas_entity_id INTEGER,
  midas_entity_is_active BOOLEAN,
  global_entity_id VARCHAR(10),
  dwh_company_id INTEGER,
  source_id INTEGER,
  -- vendor information
  midas_vendor_id INTEGER,
  midas_vendor_name VARCHAR(300),
  midas_vendor_is_active BOOLEAN,
  global_vendor_id VARCHAR(120),
  vendor_code VARCHAR(20),
  -- campaign information
  campaign_type VARCHAR(60),
  budgeted_notifications INTEGER,
  cpn_price FLOAT,
  billing_type VARCHAR(60),
  "status" VARCHAR(60),
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  -- stats and performance
  sent_notifications INTEGER,
  left_notifications INTEGER,
  paid_impressions_used BOOLEAN,
  num_orders INTEGER
)
;
