-- Vendor-User eligibility
--
-- considering:
-- - User Geo
-- - User prefered cuisine
-- - Past orders in the restaurant
TRUNCATE {{ ncr_schema }}.{{ brand_code }}_campaign_eligibility;

INSERT INTO {{ ncr_schema }}.{{ brand_code }}_campaign_eligibility
WITH user_preferred_cuisine AS (
  -- FIXME:
  -- This is a copy/paste from analytical customer segmentation, should
  -- probably be replaced by a JOIn to existent table.
  SELECT
    source_id,
    analytical_customer_id AS customer_id,
    cuisine_name_eng AS preferred_cuisine_all_rest
  FROM (
    SELECT
      source_id,
      analytical_customer_id,
      cuisine_name_eng,
      row_number() OVER (
        PARTITION BY source_id, analytical_customer_id
        -- order by most frequently and alphabetical
        ORDER BY COUNT(DISTINCT order_id) DESC, cuisine_name_eng ASC
      ) AS n_row
    FROM {{ il_schema }}.ranked_fct_order AS rfo
    JOIN {{ il_schema }}.dim_restaurant_cuisine as drc
      USING (source_id, restaurant_id)
    WHERE rfo.is_sent
      -- orders in the last year
      AND rfo.order_date >= 'today'::DATE - 365
      AND drc.main_cuisine
      AND rfo.is_whitelabel IS NOT TRUE
      AND rfo.analytical_customer_id IS NOT NULL
    GROUP BY 1, 2, 3
  )
  WHERE n_row = 1
),
user_stats AS (
  SELECT
    source_id,
    analytical_customer_id AS customer_id,
    last_order_id,
    last_order_date
  FROM {{ il_schema }}.fct_customer
),
successful_orders_per_restaurant AS (
  SELECT
    source_id,
    analytical_customer_id AS customer_id,
    restaurant_id,
    MAX(order_date) AS last_order_restaurant_date,
    COUNT(order_id) AS orders_per_restaurant
  FROM {{ il_schema }}.ranked_fct_order
  WHERE is_sent IS TRUE
  GROUP BY 1, 2, 3
),
restaurant_cuisines AS (
  SELECT *
  FROM {{ il_schema }}.dim_restaurant_cuisine
  WHERE main_cuisine IS TRUE
),
restaurant_deals AS (
    SELECT
        source_id,
        local_vendor_id AS restaurant_id,
        CURRENT_DATE AS deal_date,
        LISTAGG(local_deal_name, '|') AS active_deal_names,
        LISTAGG(local_deal_type, '|') AS active_deal_types
    FROM {{ ncr_schema }}.{{ brand_code }}_crm_vendor_feed
    NATURAL LEFT JOIN dwh_il.dim_countries
    WHERE CURRENT_DATE BETWEEN local_deal_start_date AND local_deal_end_date
    GROUP BY 1, 2
),
user_last_ncr_actions AS (
  SELECT
    source_id,
    customer_id,
    MAX(ncr_publish_date) AS last_customer_action_ts
  FROM {{ ncr_schema }}.{{ brand_code }}_ncr_braze_customer
  NATURAL INNER JOIN {{ ncr_schema }}.{{ brand_code }}_braze_history
  GROUP BY 1, 2
),
user_last_ncr_package_actions AS (
  SELECT
    source_id,
    customer_id,
    ncr_vendor_id as restaurant_id,
    MAX(CASE WHEN ncr_package_type = 'acquisition' THEN ncr_publish_date END) AS last_vendor_acquisition_action,
    MAX(CASE WHEN ncr_package_type = 'winback' THEN ncr_publish_date END) AS last_vendor_winback_action,
    MAX(CASE WHEN ncr_package_type = 'engagement' THEN ncr_publish_date END) AS last_vendor_engagement_action
  FROM {{ ncr_schema }}.{{ brand_code }}_ncr_braze_customer
  NATURAL INNER JOIN {{ ncr_schema }}.{{ brand_code }}_braze_history
  GROUP BY 1, 2, 3
),
enriched_vendor_user_map AS (
  SELECT
    source_id,
    restaurant_id,
    customer_id,
    (cuisine_name_eng = preferred_cuisine_all_rest) AS restaurant_main_cuisine_match_preferred_cuisine,
    last_order_id,
    last_order_date,
    last_order_restaurant_date,
    orders_per_restaurant,
    active_deal_names,
    COALESCE(LENGTH(active_deal_names) > 0, FALSE) AS restaurant_has_active_deals,
    -- TBD: Replace with actual values later. For now we put arbitrary values.
    COALESCE(last_customer_action_ts, DATEADD(days, -15, CURRENT_DATE)) AS last_ncr_crm_action_date,
    COALESCE(
        last_vendor_acquisition_action,
        DATEADD(days, -90, CURRENT_DATE)
    ) AS last_ncr_crm_restaurant_acquisition_action_date,
    COALESCE(
        last_vendor_winback_action,
        DATEADD(days, -90, CURRENT_DATE)
    ) AS last_ncr_crm_restaurant_winback_action_date,
    COALESCE(
        last_vendor_engagement_action,
        DATEADD(days, -90, CURRENT_DATE)
    ) AS last_ncr_crm_restaurant_engagement_action_date
  FROM {{ vendor_user_mapping_schema }}.{{ brand_code }}_vendor_user_geo_mapping AS m
  NATURAL LEFT JOIN restaurant_cuisines AS rc
  NATURAL LEFT JOIN restaurant_deals AS rd
  NATURAL LEFT JOIN user_preferred_cuisine AS pc
  NATURAL LEFT JOIN user_stats AS us
  NATURAL LEFT JOIN successful_orders_per_restaurant AS sopr
  NATURAL LEFT JOIN user_last_ncr_actions AS ulna
  NATURAL LEFT JOIN user_last_ncr_package_actions AS ulnpa
),
consolidation AS (
  SELECT
    source_id,
    restaurant_id,
    customer_id,
    restaurant_main_cuisine_match_preferred_cuisine,
    last_order_restaurant_date,
    last_order_date,
    orders_per_restaurant,
    active_deal_names,
    restaurant_has_active_deals,
    last_ncr_crm_action_date,
    last_ncr_crm_restaurant_acquisition_action_date,
    last_ncr_crm_restaurant_winback_action_date,
    last_ncr_crm_restaurant_engagement_action_date,
    GREATEST(
      last_ncr_crm_restaurant_acquisition_action_date,
      last_ncr_crm_restaurant_winback_action_date,
      last_ncr_crm_restaurant_engagement_action_date
    ) AS last_ncr_crm_restaurant_action_date,
    DATEDIFF(days, last_ncr_crm_action_date, CURRENT_DATE) > 2 AS user_not_spammed,
    (
      last_order_restaurant_date IS NULL
      AND DATEDIFF(days, last_order_date, CURRENT_DATE) < 30
      AND restaurant_main_cuisine_match_preferred_cuisine IS TRUE
      AND restaurant_has_active_deals
      AND user_not_spammed
      AND DATEDIFF(days, last_ncr_crm_restaurant_acquisition_action_date, CURRENT_DATE) > 30
    ) AS eligible_for_aquisition,
    (
      orders_per_restaurant >= 1
      AND last_order_restaurant_date IS NOT NULL
      AND DATEDIFF(days, last_order_date, CURRENT_DATE) >= 30
      AND restaurant_has_active_deals
      AND user_not_spammed
      AND DATEDIFF(days, last_ncr_crm_restaurant_winback_action_date, CURRENT_DATE) > 30
    ) AS eligible_for_winback,
    (
      orders_per_restaurant >= 1
      AND last_order_restaurant_date IS NOT NULL
      AND DATEDIFF(days, last_order_date, CURRENT_DATE) < 30
      AND restaurant_has_active_deals
      AND user_not_spammed
      AND DATEDIFF(days, last_ncr_crm_restaurant_engagement_action_date, CURRENT_DATE) > 30
    ) AS eligible_for_engagement,
    -- Currently all campaigns types are mutually exclusive.
    -- A single user can only be eligible for at most one campaign type per restaurant.
    -- We can use this fact to make the JOIN easy using `campaign_eligibility`
    CASE
      WHEN eligible_for_aquisition
        THEN 'acquisition'
      WHEN eligible_for_winback
        THEN 'winback'
      WHEN eligible_for_engagement
        THEN 'engagement'
    END AS campaign_eligibility
  FROM enriched_vendor_user_map
)
SELECT *
FROM consolidation
;
