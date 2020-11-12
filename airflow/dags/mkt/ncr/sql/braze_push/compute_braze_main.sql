-- Final braze attributes table used to compute payloads sent to braze.
INSERT INTO {{ ncr_schema }}.{{ brand_code }}_braze_main
WITH ncr_external_ids AS (
SELECT
    mapp.braze_external_id,
    eng.midas_booking_id,
    eng.source_id,
    eng.customer_id,
    eng.global_entity_id AS source_code,
    eng.campaign_type AS ncr_package_type,
    eng.restaurant_id AS ncr_vendor_id,
    '{{ next_ds }}'::date AS ncr_publish_date,
    -- This is implemented to deduplicate talabat records as multiple
    -- acids can be set for one e-mail(external_id)
    ROW_NUMBER() OVER (
        PARTITION BY mapp.braze_external_id, eng.source_id
        ORDER BY RANDOM()
    ) as external_id_occurence
FROM {{ ncr_schema }}.{{ brand_code }}_decision_engine_result AS eng
-- Inner join here to get rid of records of campaigns with no eligible users (customer_id == NULL)
JOIN {{ ncr_schema }}.{{ brand_code }}_ncr_braze_customer AS mapp
    ON eng.customer_id = mapp.customer_id
    AND eng.source_id = mapp.source_id
)
SELECT
    braze_external_id,
    midas_booking_id,
    source_id,
    source_code,
    customer_id,
    ncr_package_type,
    ncr_vendor_id,
    ncr_publish_date
FROM ncr_external_ids
WHERE external_id_occurence = 1
;
