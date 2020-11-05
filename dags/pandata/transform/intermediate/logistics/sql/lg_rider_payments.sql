WITH rider_payments_agg_hidden_basic_by_rule AS (
  SELECT
    rider_payments.created_date,
    rider_payments.country_code,
    rider_payments.rider_id,
    ARRAY_AGG(
      STRUCT(
        `{project_id}`.pandata_intermediate.LG_UUID(hidden_basic_by_rule.payment_rule_id, rider_payments.country_code) AS lg_payments_basic_rule_uuid,
        hidden_basic_by_rule.payment_rule_id AS lg_payments_basic_rule_id,
        hidden_basic_by_rule.total AS total_local,
        hidden_basic_by_rule.total_eur,
        hidden_basic_by_rule.amount AS payment_unit_count
      )
    ) AS hidden_basic_by_rule
  FROM `fulfillment-dwh-production.curated_data_shared.rider_payments` AS rider_payments
  CROSS JOIN UNNEST (rider_payments.payment_details.hidden_basic_by_rule) AS hidden_basic_by_rule
  GROUP BY
    rider_payments.created_date,
    rider_payments.country_code,
    rider_payments.rider_id
),

rider_payments_agg_hidden_basic AS (
  SELECT
    rider_payments.created_date,
    rider_payments.country_code,
    rider_payments.rider_id,
    ARRAY_AGG(
      STRUCT(
        hidden_basic.payment_id AS lg_payment_id,
        `{project_id}`.pandata_intermediate.LG_UUID(hidden_basic.payment_rule_id, rider_payments.country_code) AS lg_payments_basic_rule_uuid,
        hidden_basic.payment_rule_id AS lg_payments_basic_rule_id,
        hidden_basic.delivery_id AS lg_delivery_id,
        hidden_basic.shift_id AS lg_shift_id,
        hidden_basic.payment_cycle_id AS lg_payment_cycle_id,
        hidden_basic.status,
        hidden_basic.payment_type AS payment_unit_type,
        hidden_basic.payment_rule_name,
        hidden_basic.city_name,
        hidden_basic.total AS total_local,
        hidden_basic.total_eur,
        hidden_basic.amount AS payment_unit_count,
        hidden_basic.created_at AS created_at_utc,
        hidden_basic.payment_cycle_start_date AS payment_cycle_start_at_utc,
        hidden_basic.payment_cycle_end_date AS payment_cycle_end_at_utc
      )
    ) AS hidden_basic
  FROM `fulfillment-dwh-production.curated_data_shared.rider_payments` AS rider_payments
  CROSS JOIN UNNEST (rider_payments.payment_details.hidden_basic) AS hidden_basic
  GROUP BY
    rider_payments.created_date,
    rider_payments.country_code,
    rider_payments.rider_id
),

rider_payments_agg_scoring AS (
  SELECT
    rider_payments.created_date,
    rider_payments.country_code,
    rider_payments.rider_id,
    ARRAY_AGG(
      STRUCT(
        scoring.payment_id AS lg_payment_id,
        `{project_id}`.pandata_intermediate.LG_UUID(scoring.payment_rule_id, rider_payments.country_code) AS lg_payments_scoring_rule_uuid,
        scoring.payment_rule_id AS lg_payments_scoring_rule_id,
        scoring.goal_id AS lg_goal_id,
        scoring.payment_cycle_id AS lg_payment_cycle_id,
        scoring.delivery_id AS lg_delivery_id,
        scoring.evaluation_id AS lg_evaluation_id,

        scoring.status,
        scoring.city_name,
        scoring.scoring_amount,
        scoring.goal_type,
        scoring.payment_type AS payment_unit_type,
        scoring.paid_unit_amount AS payment_unit_count,

        scoring.total AS total_local,
        scoring.total_eur,

        scoring.created_at AS created_at_utc,
        scoring.paid_period_start AS paid_period_start_utc,
        scoring.paid_period_end AS paid_period_end_utc,
        scoring.payment_cycle_start_date AS payment_cycle_start_at_utc,
        scoring.payment_cycle_end_date AS payment_cycle_end_at_utc
      )
    ) AS scoring
  FROM `fulfillment-dwh-production.curated_data_shared.rider_payments` AS rider_payments
  CROSS JOIN UNNEST (rider_payments.payment_details.scoring) AS scoring
  GROUP BY
    rider_payments.created_date,
    rider_payments.country_code,
    rider_payments.rider_id
),

rider_payments_agg_quest_by_rule AS (
  SELECT
    rider_payments.created_date,
    rider_payments.country_code,
    rider_payments.rider_id,
    ARRAY_AGG(
      STRUCT(
        `{project_id}`.pandata_intermediate.LG_UUID(quest_by_rule.payment_rule_id, rider_payments.country_code) AS lg_payments_quest_rule_uuid,
        quest_by_rule.payment_rule_id AS lg_payments_quest_rule_id,
        quest_by_rule.total AS total_local,
        quest_by_rule.total_eur,
        quest_by_rule.amount AS payment_unit_count
      )
    ) AS quest_by_rule
  FROM `fulfillment-dwh-production.curated_data_shared.rider_payments` AS rider_payments
  CROSS JOIN UNNEST (rider_payments.payment_details.quest_by_rule) AS quest_by_rule
  GROUP BY
    rider_payments.created_date,
    rider_payments.country_code,
    rider_payments.rider_id
),

rider_payments_agg_quest AS (
  SELECT
    rider_payments.created_date,
    rider_payments.country_code,
    rider_payments.rider_id,
    ARRAY_AGG(
      STRUCT(
        quest.payment_id AS lg_payment_id,
        `{project_id}`.pandata_intermediate.LG_UUID(quest.payment_rule_id, rider_payments.country_code) AS lg_payments_quest_rule_uuid,
        quest.payment_rule_id AS lg_payments_quest_rule_id,
        quest.goal_id AS lg_goal_id,
        quest.payment_cycle_id AS lg_payment_cycle_id,

        quest.status,
        quest.city_name,

        quest.goal_type,
        quest.payment_type AS payment_unit_type,
        quest.amount AS payment_unit_count,

        quest.quest_payment_rule_name AS payment_rule_name,
        quest.duration AS duration_in_iso8601,
        quest.total AS total_local,
        quest.total_eur,
        quest.threshold AS payment_unit_threshold,
        quest.accepted_deliveries AS deliveries_accepted_count,
        quest.notified_deliveries AS deliveries_notified_count,
        quest.no_shows AS no_show_count,
        quest.created_at AS created_at_utc,
        quest.paid_period_start AS paid_period_start_utc,
        quest.paid_period_end AS paid_period_end_utc,
        quest.payment_cycle_start_date AS payment_cycle_start_at_utc,
        quest.payment_cycle_end_date AS payment_cycle_end_at_utc
      )
    ) AS quest
  FROM `fulfillment-dwh-production.curated_data_shared.rider_payments` AS rider_payments
  CROSS JOIN UNNEST (rider_payments.payment_details.quest) AS quest
  GROUP BY
    rider_payments.created_date,
    rider_payments.country_code,
    rider_payments.rider_id
),

rider_payments_agg_basic_by_rule AS (
  SELECT
    rider_payments.created_date,
    rider_payments.country_code,
    rider_payments.rider_id,
    ARRAY_AGG(
      STRUCT(
        `{project_id}`.pandata_intermediate.LG_UUID(basic_by_rule.payment_rule_id, rider_payments.country_code) AS lg_payments_basic_rule_uuid,
        basic_by_rule.payment_rule_id AS lg_payments_basic_rule_id,
        basic_by_rule.total AS total_local,
        basic_by_rule.total_eur,
        basic_by_rule.amount AS payment_unit_count
      )
    ) AS basic_by_rule
  FROM `fulfillment-dwh-production.curated_data_shared.rider_payments` AS rider_payments
  CROSS JOIN UNNEST (rider_payments.payment_details.basic_by_rule) AS basic_by_rule
  GROUP BY
    rider_payments.created_date,
    rider_payments.country_code,
    rider_payments.rider_id
),

rider_payments_agg_basic AS (
  SELECT
    rider_payments.created_date,
    rider_payments.country_code,
    rider_payments.rider_id,
    ARRAY_AGG(
      STRUCT(
        `{project_id}`.pandata_intermediate.LG_UUID(basic.payment_rule_id, rider_payments.country_code) AS lg_payments_basic_rule_uuid,
        basic.payment_id AS lg_payment_id,
        basic.payment_rule_id AS lg_payments_basic_rule_id,
        basic.delivery_id AS lg_delivery_id,
        basic.shift_id AS lg_shift_id,
        basic.payment_cycle_id AS lg_payment_cycle_id,
        basic.status,
        basic.payment_type AS payment_unit_type,
        basic.payment_rule_name,
        basic.city_name,
        basic.total AS total_local,
        basic.total_eur,
        basic.amount AS payment_unit_count,
        basic.created_at AS created_at_utc,
        basic.payment_cycle_start_date AS payment_cycle_start_at_utc,
        basic.payment_cycle_end_date AS payment_cycle_end_at_utc
      )
    ) AS basic
  FROM `fulfillment-dwh-production.curated_data_shared.rider_payments` AS rider_payments
  CROSS JOIN UNNEST (rider_payments.payment_details.basic) AS basic
  GROUP BY
    rider_payments.created_date,
    rider_payments.country_code,
    rider_payments.rider_id
)

SELECT
  (
    CAST(rider_payments.created_date AS STRING) || '_' ||
    `{project_id}`.pandata_intermediate.LG_UUID(rider_payments.rider_id, rider_payments.country_code)
  ) AS uuid,
  `{project_id}`.pandata_intermediate.LG_UUID(rider_payments.rider_id, rider_payments.country_code) AS lg_rider_uuid,
  rider_payments.rider_id AS lg_rider_id,

  rider_payments.rider_name,

  `{project_id}`.pandata_intermediate.EXCLUDE_T2(rider_payments.country_code) AS country_code,

  rider_payments.timezone,
  rider_payments.exchange_rate,
  rider_payments.total_date_payment_eur AS total_payment_eur,
  rider_payments.total_date_payment AS total_payment_local,
  rider_payments.created_date AS created_date_utc,
  STRUCT(
    rider_payments_agg_basic.basic,
    rider_payments_agg_basic_by_rule.basic_by_rule,
    rider_payments_agg_hidden_basic.hidden_basic,
    rider_payments_agg_hidden_basic_by_rule.hidden_basic_by_rule,
    rider_payments_agg_quest.quest,
    rider_payments_agg_quest_by_rule.quest_by_rule,
    rider_payments_agg_scoring.scoring
  ) AS details,
FROM `fulfillment-dwh-production.curated_data_shared.rider_payments` AS rider_payments
LEFT JOIN rider_payments_agg_basic
       ON rider_payments.created_date = rider_payments_agg_basic.created_date
      AND rider_payments.country_code = rider_payments_agg_basic.country_code
      AND rider_payments.rider_id = rider_payments_agg_basic.rider_id
LEFT JOIN rider_payments_agg_hidden_basic
       ON rider_payments.created_date = rider_payments_agg_hidden_basic.created_date
      AND rider_payments.country_code = rider_payments_agg_hidden_basic.country_code
      AND rider_payments.rider_id = rider_payments_agg_hidden_basic.rider_id
LEFT JOIN rider_payments_agg_hidden_basic_by_rule
       ON rider_payments.created_date = rider_payments_agg_hidden_basic_by_rule.created_date
      AND rider_payments.country_code = rider_payments_agg_hidden_basic_by_rule.country_code
      AND rider_payments.rider_id = rider_payments_agg_hidden_basic.rider_id
LEFT JOIN rider_payments_agg_basic_by_rule
       ON rider_payments.created_date = rider_payments_agg_basic_by_rule.created_date
      AND rider_payments.country_code = rider_payments_agg_basic_by_rule.country_code
      AND rider_payments.rider_id = rider_payments_agg_basic_by_rule.rider_id
LEFT JOIN rider_payments_agg_quest
       ON rider_payments.created_date = rider_payments_agg_quest.created_date
      AND rider_payments.country_code = rider_payments_agg_quest.country_code
      AND rider_payments.rider_id = rider_payments_agg_quest.rider_id
LEFT JOIN rider_payments_agg_quest_by_rule
       ON rider_payments.created_date = rider_payments_agg_quest_by_rule.created_date
      AND rider_payments.country_code = rider_payments_agg_quest_by_rule.country_code
      AND rider_payments.rider_id = rider_payments_agg_quest_by_rule.rider_id
LEFT JOIN rider_payments_agg_scoring
       ON rider_payments.created_date = rider_payments_agg_scoring.created_date
      AND rider_payments.country_code = rider_payments_agg_scoring.country_code
      AND rider_payments.rider_id = rider_payments_agg_scoring.rider_id
