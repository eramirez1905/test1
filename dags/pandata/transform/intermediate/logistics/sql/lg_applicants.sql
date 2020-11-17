WITH stages_agg_transitions AS  (
  SELECT
    applicants.applicant_id,
    tiers.tier,
    stages.index_in_tier,
    stages.name,
    ARRAY_AGG(
      STRUCT(
        transitions.to_state,
        transitions.created_at AS created_at_utc,
        TIMESTAMP(DATETIME(transitions.created_at, applicants.timezone)) AS created_at_local
      )
    ) AS transitions
  FROM `fulfillment-dwh-production.curated_data_shared.applicants` AS applicants
  CROSS JOIN UNNEST(applicants.workflow.tiers) AS tiers
  CROSS JOIN UNNEST(tiers.stage) AS stages
  CROSS JOIN UNNEST(stages.transitions) AS transitions
  GROUP BY
    applicants.applicant_id,
    tiers.tier,
    stages.index_in_tier,
    stages.name
),

tiers_agg_stages AS (
  SELECT
    applicants.applicant_id,
    tiers.tier,
    ARRAY_AGG(
      STRUCT(
        stages.index_in_tier,
        stages.name,
        stages.type,
        stages_agg_transitions.transitions
      )
    ) AS stages
  FROM `fulfillment-dwh-production.curated_data_shared.applicants` AS applicants
  CROSS JOIN UNNEST(applicants.workflow.tiers) AS tiers
  CROSS JOIN UNNEST(tiers.stage) AS stages
  LEFT JOIN stages_agg_transitions
         ON applicants.applicant_id = stages_agg_transitions.applicant_id
        AND tiers.tier = stages_agg_transitions.tier
        AND stages.index_in_tier = stages_agg_transitions.index_in_tier
        AND stages.name = stages_agg_transitions.name
  GROUP BY
    applicants.applicant_id,
    tiers.tier
),

applicants_agg_tiers AS (
  SELECT
    applicants.applicant_id,
    ARRAY_AGG(
      STRUCT(
        tiers.tier,
        tiers_agg_stages.stages
      )
    ) AS tiers
  FROM `fulfillment-dwh-production.curated_data_shared.applicants` AS applicants
  CROSS JOIN UNNEST(applicants.workflow.tiers) AS tiers
  LEFT JOIN tiers_agg_stages
         ON applicants.applicant_id = tiers_agg_stages.applicant_id
        AND tiers.tier = tiers_agg_stages.tier
  GROUP BY applicants.applicant_id
)

SELECT
  applicants.applicant_id AS uuid,
  applicants.applicant_id AS id,
  `{project_id}`.pandata_intermediate.LG_UUID(applicants.referred_by_id, applicants.country_code) AS referrer_lg_rider_uuid,
  applicants.referred_by_id AS referrer_lg_rider_id,
  `{project_id}`.pandata_intermediate.LG_UUID(
    `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(applicants.country_code, applicants.city_id),
    applicants.country_code
  ) AS lg_city_uuid,
  `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(applicants.country_code, applicants.city_id) AS lg_city_id,
  applicants.language_id AS lg_language_id,
  `{project_id}`.pandata_intermediate.LG_UUID(applicants.rider_id, applicants.country_code) AS lg_rider_uuid,
  applicants.rider_id AS lg_rider_id,
  applicants.region,
  `{project_id}`.pandata_intermediate.EXCLUDE_T2(applicants.country_code) AS country_code,
  applicants.url_id,

  applicants.duplicate AS is_duplicate,
  applicants.braze_welcome_sent AS is_braze_welcome_sent,

  applicants.rejection_category AS rejection_category_type,
  applicants.rejection_type,
  applicants.link_type,

  applicants.url,
  applicants.link_click_count,

  applicants.timezone,
  applicants.created_date AS created_date_utc,
  TIMESTAMP(DATETIME(applicants.created_at, applicants.timezone)) AS created_at_local,
  applicants.created_at AS created_at_utc,
  TIMESTAMP(DATETIME(applicants.braze_user_created_at, applicants.timezone)) AS braze_user_created_at_local,
  applicants.braze_user_created_at AS braze_user_created_at_utc,
  TIMESTAMP(DATETIME(applicants.last_reminded_at, applicants.timezone)) AS last_reminded_at_local,
  applicants.last_reminded_at AS last_reminded_at_utc,
  TIMESTAMP(DATETIME(applicants.rejected_at, applicants.timezone)) AS rejected_at_local,
  applicants.rejected_at AS rejected_at_utc,
  TIMESTAMP(DATETIME(applicants.approved_at, timezone)) AS approved_at_local,
  applicants.approved_at AS approved_at_utc,
  applicants.custom_fields,
  STRUCT(
    applicants.workflow.workflow_name AS name,
    applicants.workflow.max_idle_days AS max_idle_in_days,
    applicants.workflow.idle_reminder_delay AS workflow_idle_reminder_delay_in_days,
    applicants_agg_tiers.tiers
  ) AS workflow,
FROM `fulfillment-dwh-production.curated_data_shared.applicants` AS applicants
LEFT JOIN applicants_agg_tiers
       ON applicants.applicant_id = applicants_agg_tiers.applicant_id
