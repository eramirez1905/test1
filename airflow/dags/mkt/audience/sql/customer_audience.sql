/*
This script computes the audience for a customer

- (Unique) Customers are identified by analytical_customer_id (acid) and assigned a master_id
- master_id is designed to be invariant under analytical_customer_id changes (e.g. when customers get merged or acid gets recomputed by DWH)
- master_id is a (unique) primary key in all tables involged in this script
- Only customers with at least one (successful) order are considered
    - FIXME: Currently, having at least one (not neccesarily successful) order is required for the master_id computation
    - FIXME: It remains unclear, how we will be able to handle "prospects" with this setup
- The first time we see a customer we assign it a random number. This random number is used to assign customers to variants of a campaign.
  It should not change in a customer lifecycle to avoid having customer see multiple variations of the same campaign.

Naming conventions:
- Stage:
    Is based on customer metrics date_of_last_purchase, total_number_of_orders and returning probability.
- Stage Age:
    How long a user has been member of certain stage will be used in Campaing definitions.
- Variant:
    Essentially derived from random_number. However the definition also depends on stage membership.
- Channel:
    CRM (Braze) or Performance Marketing (mParticle)
- Campaign:
    Orchestrates which campaign is shown to the customer based on stage, stage age and variant by channel.
- Audience:
    Name of the Audience as string. Incroporates source_code, stage, variant, channel and campaign and will be used to set up the campaigns in Braze and mParticle accordingly by marketeers.

Implementations
- Stage and Variant are implemented as UDFs
- Channel, Campaign, Audiences are implemented as static mapping table

*/
DROP TABLE IF EXISTS customer_stage_variant;
CREATE TEMPORARY TABLE customer_stage_variant
    DISTSTYLE KEY
    DISTKEY ( master_id )
AS
SELECT
    cs.source_id,
    cs.source_code,
    cs.analytical_customer_id,
    cs.master_id,
    cs.customer_id,
    cs.last_order_ts,
    cs.loyalty_status_all_verts,
    cs.total_orders,
    cs.total_non_restaurant_orders,
    cs.stage,
    cs.stage_age,
    cs.random_num,
    avs.variant
FROM
    {{ audience_schema }}.{{ brand_code }}_customer_stage AS cs
    LEFT JOIN {{ audience_schema }}.{{ brand_code }}_audience_variant_split AS avs
      ON
        cs.source_id = avs.source_id AND cs.stage = avs.stage
        AND (avs.split_from <= cs.random_num AND cs.random_num < avs.split_to)
        AND avs.valid_until IS NULL
;

TRUNCATE TABLE {{ audience_schema }}.{{ brand_code }}_customer_audience;
INSERT INTO {{ audience_schema }}.{{ brand_code }}_customer_audience
SELECT
    csv.source_id,
    csv.source_code,
    csv.analytical_customer_id,
    csv.master_id,
    csv.customer_id,
    UPPER(vd.channel) AS channel,
    CASE
        WHEN vd.campaign = 'None' THEN 'Exclude'
        ELSE dc.dwh_source_code || '_' || UPPER(vd.channel) || '_' || vd.stage || '_' || vd.variant || '_' || vd.campaign
    END AS audience
FROM customer_stage_variant AS csv
JOIN {{ audience_schema }}.{{ brand_code }}_audience_definition AS vd
    -- mandatory Audience definitions
    ON csv.source_id = vd.source_id
    AND csv.stage = vd.stage
    AND csv.variant = vd.variant
    AND csv.stage_age BETWEEN vd.days_in_stage_enter AND vd.days_in_stage_exit
    -- optional (sub-) Audience definitions
    AND (vd.loyalty_status_all_verts IS NULL OR csv.loyalty_status_all_verts = vd.loyalty_status_all_verts)
    AND (vd.total_non_restaurant_orders IS NULL OR csv.total_non_restaurant_orders = vd.total_non_restaurant_orders)
    -- filter disabled Audiences
    AND vd.valid_until IS NULL
LEFT JOIN dwh_il.dim_countries AS dc
    ON csv.source_id = dc.source_id
;


SET analyze_threshold_percent TO 0;
ANALYZE {{ audience_schema }}.{{ brand_code }}_customer_audience
PREDICATE COLUMNS;
