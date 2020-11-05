/*
Enrich and filter NCR bookings from MIDAS for later use

status:
new: created but not approved (not live)
open; active
cancelled: not active, terminated
*/
INSERT INTO {{ ncr_schema }}.{{ brand_code }}_ncr_midas_campaign_bookings (
  SELECT
    -- midas id
    b.midas_booking_id,
    -- entity information
    b.midas_entity_id,
    b.midas_entity_is_active,
    b.global_entity_id,
    dc.dwh_company_id,
    dc.source_id,
    -- vendor information
    b.midas_vendor_id,
    b.midas_vendor_name,
    b.midas_vendor_is_active,
    b.global_vendor_id,
    b.vendor_code,
    -- campaign information
    b.campaign_type,
    b.budgeted_notifications,
    b.cpn_price,
    b.billing_type,
    b."status",
    b.started_at,
    b.ended_at,
    -- stats and performance
    b.sent_notifications,
    b.left_notifications,
    b.paid_impressions_used,
    b.num_orders

  FROM {{ ncr_schema }}.{{ brand_code }}_st_ncr_midas_campaign_bookings AS b
  LEFT JOIN dwh_il.dim_countries AS dc
      ON b.global_entity_id = dc.dwh_source_code
  WHERE
    "status" IN ('open')
    AND global_entity_id IN {{ source_codes | sql_list_filter }}
    AND dc.is_active IS TRUE
)
;
