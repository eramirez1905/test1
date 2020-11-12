CREATE TABLE IF NOT EXISTS {{ ncr_schema }}.{{ brand_code }}_campaign_eligibility (
  source_id INTEGER,
  restaurant_id VARCHAR(20),
  customer_id VARCHAR(64),
  restaurant_main_cuisine_match_preferred_cuisine BOOLEAN,
  last_order_restaurant_date TIMESTAMP,
  last_order_date TIMESTAMP,
  orders_per_restaurant BIGINT,
  active_deal_names VARCHAR(2048),
  restaurant_has_active_crm_deals BOOLEAN,
  last_ncr_crm_action_date TIMESTAMP,
  last_ncr_crm_restaurant_acquisition_action_date TIMESTAMP,
  last_ncr_crm_restaurant_winback_action_date TIMESTAMP,
  last_ncr_crm_restaurant_engagement_action_date TIMESTAMP,
  last_ncr_crm_restaurant_action_date TIMESTAMP,
  user_not_spammed BOOLEAN,
  eligible_for_aquisition BOOLEAN,
  eligible_for_winback BOOLEAN,
  eligible_for_engagement BOOLEAN,
  campaign_eligibility VARCHAR(60)
)
;
