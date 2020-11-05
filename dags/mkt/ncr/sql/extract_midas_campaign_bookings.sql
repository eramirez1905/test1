SELECT
  -- midas id
  booking_id AS midas_booking_id,
  -- entity information
  entity_id AS midas_entity_id,
  e.is_active AS midas_entity_is_active,
  global_entity_id,
  -- vendor information
  vendor_id AS midas_vendor_id,
  v."name" AS midas_vendor_name,
  v.is_active AS midas_vendor_is_active,
  global_vendor_id,
  vendor_code,
  -- campaign information
  campaign AS campaign_type,
  budgeted_notifications,
  cpn_price,
  billing_type,
  status,
  bo.started_at,
  bo.ended_at,
  -- stats and performance
  sent_notifications,
  GREATEST(0, budgeted_notifications - sent_notifications) AS left_notifications,
  paid_impressions_used,
  num_orders
FROM app_data.marketing_tech_billing_cpn AS bi
LEFT JOIN app_data.marketing_tech_bookings AS bo
  ON bi.booking_id = bo.id
LEFT JOIN app_data.marketing_tech_vendors AS v
  ON bo.vendor_id = v.id
LEFT JOIN app_data.marketing_tech_entities AS e
  ON v.entity_id = e.id
;
