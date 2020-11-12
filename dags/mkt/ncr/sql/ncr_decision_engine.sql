--
-- Run the NCR decision engine
--
-- Information about the decision engine logic can be found in ncr/docs/decision_engine.md
--
SET SEED TO 0.5;

TRUNCATE {{ ncr_schema }}.{{ brand_code }}_decision_engine_result;

INSERT INTO {{ ncr_schema }}.{{ brand_code }}_decision_engine_result
WITH vendor_user_ncr_campaign_mapping AS (
  SELECT
    b.midas_booking_id,
    b.vendor_code AS restaurant_id,
    b.global_entity_id,
    b.source_id,
    b.campaign_type,
    b.left_notifications,
    -- Logic below is to limit number of notifications sent daily as not to overwhelm
    -- restaurants with too many orders. 5% is added on top to account for control group
    CEILING(
      CASE
        WHEN b.left_notifications > (b.budgeted_notifications / 3.0)
          THEN (b.budgeted_notifications / 3.0) * 1.05
        ELSE b.left_notifications * 1.05
      END
    )::INT AS max_notifications_to_send,
    --
    e.customer_id,
    --
    COUNT(e.customer_id) OVER (
      PARTITION BY e.customer_id
    ) AS campaigns_eligible_for,
    COUNT(e.customer_id) OVER (
      PARTITION BY b.source_id, restaurant_id, b.campaign_type
    ) AS campaign_targetable_customers,
    1.0 * max_notifications_to_send / NULLIF(campaign_targetable_customers, 0) AS decision_factor
  FROM {{ ncr_schema }}.{{ brand_code }}_ncr_midas_campaign_bookings AS b
  LEFT JOIN {{ ncr_schema }}.{{ brand_code }}_campaign_eligibility AS e
    ON b.source_id = e.source_id
    AND b.vendor_code = e.restaurant_id
    AND b.campaign_type = e.campaign_eligibility
  WHERE
    '{{ next_ds }}' BETWEEN b.started_at AND b.ended_at
),
vendor_user_ncr_campaign_mapping_factor AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY customer_id
      ORDER BY decision_factor DESC
    ) AS factor_rank
  FROM vendor_user_ncr_campaign_mapping
),
vendor_user_ncr_campaign_mapping_ranking AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY source_id, restaurant_id, campaign_type
      ORDER BY RANDOM()
    ) AS random_top_n
  FROM vendor_user_ncr_campaign_mapping_factor
  WHERE factor_rank = 1
),
decision_engine_result AS (
  SELECT *
  FROM vendor_user_ncr_campaign_mapping_ranking
  WHERE random_top_n <= max_notifications_to_send
)
SELECT *
FROM decision_engine_result
;
