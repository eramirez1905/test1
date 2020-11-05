TRUNCATE {{ ncr_schema }}.{{ brand_code }}_braze_campaigns;

INSERT INTO {{ ncr_schema }}.{{ brand_code }}_braze_campaigns (
    braze_external_id,
    midas_booking_id,
    source_id,
    entity_id,
    customer_id,
    ncr_package_type,
    ncr_vendor_id,
    ncr_publish_date,
    campaign_name,
    last_received,
    in_control,
    opened_notification
)
WITH enriched_user_campaigns_received AS (
-- Filter for only NCR campaigns
-- add the campaign type to be used in later JOIN
SELECT *
FROM {{ ncr_schema }}.{{ brand_code }}_braze_campaign_id_mapping AS ncr
LEFT JOIN {{ crm_schema }}.user_campaigns_received AS braze
    ON ncr.braze_campaign_id = braze.api_campaign_id
)
SELECT
    braze.braze_external_id,
    braze.midas_booking_id,
    braze.source_id,
    braze.source_code AS entity_id,
    braze.customer_id,
    braze.ncr_package_type,
    braze.ncr_vendor_id,
    braze.ncr_publish_date,
    campaigns.name AS campaign_name,
    campaigns.last_received,
    campaigns.in_control,
    (opened_push OR clicked_email OR opened_email OR clicked_in_app_message) AS opened_notification
FROM {{ ncr_schema }}.{{ brand_code }}_braze_history AS braze
LEFT JOIN enriched_user_campaigns_received AS campaigns
    ON braze.ncr_package_type = campaigns.campaign_type
    AND braze.braze_external_id = campaigns.external_id
    AND campaigns.last_received BETWEEN braze.valid_at AND DATEADD(day, 2, braze.valid_at)
;
