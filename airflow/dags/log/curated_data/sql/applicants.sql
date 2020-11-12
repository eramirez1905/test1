CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.applicants`
PARTITION BY created_date AS
WITH countries AS (
  SELECT c.country_code
    , c.country_name
    -- taking one timezone per country to use in the final table
    , (SELECT timezone FROM UNNEST(cities) ci ORDER BY id LIMIT 1) AS timezone
  FROM `{{ params.project_id }}.cl.countries` c
), applicants AS(
  SELECT a.region
    , w.country_configuration_id AS country_code
    , ci.timezone
    , a.location_id
    , a.id AS applicant_id
    , a.created_date
    , a.created_at
    , a.city_id
    , a.language_id
    , a.duplicate
    , a.rooster_id AS rider_id
    , a.braze_user_created_at
    , a.braze_welcome_sent
    , a.last_reminded_at
    , a.rejected_at
    , rr.category AS rejection_category
    , rr.rejection_type
    , a.approved_at
    , w.id AS workflow_id
    , a.rejected_by
    , SUBSTR(a.portal_url, LENGTH(a.portal_url) -6, 7) AS url_id
    , STRUCT(w.name AS workflow_name
        , w.max_idle_days
        , idle_reminder_delay
      ) AS workflow
    , STRUCT(rr.category
        , rr.rejection_type
      ) AS rejection
  FROM `{{ params.project_id }}.dl.arara_applicants` a
  LEFT JOIN `{{ params.project_id }}.dl.arara_workflows` w ON w.region = a.region
    AND w.id = a.workflow_id
  LEFT JOIN `{{ params.project_id }}.dl.arara_rejection_reasons` rr ON rr.region = a.region
    AND rr.id = a.rejection_reason_id
  LEFT JOIN countries ci ON w.country_configuration_id = ci.country_code
), applicant_fields_whitelisted AS (
  SELECT region
    , applicant_id
    , field_type
    , name
    , value
  FROM `{{ params.project_id }}.dl.arara_applicant_fields`
  WHERE name IN ('utm_medium', 'utm_source', 'utm_term'
    , 'utm_content', 'utm_campaign', 'city', 'area'
    , 'country', 'language', 'vehicle', 'age_check'
    , 'vehicle_check', 'gender', 'app_user', 'bot_user'
    , 'source', 'rejection_reason', 'starting_point', 'location'
    , 'on_hold_reason', 'onboarding_meeting_showed_up', 'online_onboarding_showed_up'
    , 'training_ride_showed_up', 'information_session_showed_up', 'onboarded_by'
  )
), applicant_fields AS (
  SELECT region
    , applicant_id
    , ARRAY_AGG(
        STRUCT(af.field_type AS type
          , af.name
          , af.value
        ) ORDER BY name
      ) AS custom_fields
  FROM applicant_fields_whitelisted af
  GROUP BY 1,2
), states AS (
  SELECT s.region
    , ast.applicant_id
    , s.workflow_id
    , tier_index AS tier
    , index_in_tier
    , s.name AS stage_name
    , s.stage_type
    , ARRAY_AGG(
        STRUCT(t.to_state
          , t.created_at
        ) ORDER BY t.created_at
      ) AS transitions
  FROM `{{ params.project_id }}.dl.arara_applicants` a
  LEFT JOIN `{{ params.project_id }}.dl.arara_applicant_stages` ast ON ast.region = a.region
    AND ast.applicant_id = a.id
  LEFT JOIN `{{ params.project_id }}.dl.arara_stages` s ON ast.region = s.region
    AND ast.stage_id = s.id
  LEFT JOIN `{{ params.project_id }}.dl.arara_applicant_stage_transitions` t ON t.region = ast.region
    AND t.applicant_stage_id = ast.id
  GROUP BY 1,2,3,4,5,6,7
), stages AS(
  SELECT region
    , applicant_id
    , workflow_id
    , tier
    , ARRAY_AGG(
        STRUCT(index_in_tier
          , stage_name AS name
          , stage_type AS type
          , transitions
        ) ORDER BY index_in_tier
      ) AS stage
  FROM states
  GROUP BY 1,2,3,4
), tiers AS(
  SELECT region
    , applicant_id
    , workflow_id
    , ARRAY_AGG(
        STRUCT(tier
          , stage
        ) ORDER BY tier
      ) AS tiers
  FROM stages
  GROUP BY 1, 2, 3
), urls AS (
  SELECT r.id AS url_id
    , r.value AS url
    , r.viewCount AS link_click_count
    , IF(r.value LIKE '%usehurrier.com/dashboard/arara/public/application%', 'Portal', 'Unknown') AS link_type
  FROM `{{ params.project_id }}.dl.recruitment_goldcrest_urls` AS r
)
SELECT a.region
  , a.country_code
  , a.timezone
  , a.location_id
  , a.applicant_id
  , a.created_date
  , a.created_at
  , c.external_id AS city_id
  , l.name AS location_name
  , a.language_id
  , a.duplicate
  , a.rider_id
  , a.braze_user_created_at
  , a.braze_welcome_sent
  , a.last_reminded_at
  , a.rejected_at
  , a.rejected_by
  , a.rejection_category
  , a.rejection_type
  , a.approved_at
  , CAST(
        REGEXP_EXTRACT(
            (
             SELECT value
             FROM af.custom_fields
             WHERE name = 'utm_content' AND REGEXP_CONTAINS(value, r'raf_[0-9]+')
            )
            , r'raf_([0-9]+)'
        )
        AS INT64) AS referred_by_id
  , af.custom_fields
  , STRUCT(workflow.workflow_name
      , workflow.max_idle_days
      , workflow.idle_reminder_delay
      , tiers
    ) AS workflow
  , u.url_id
  , u.url
  , u.link_click_count
  , u.link_type
FROM applicants a
LEFT JOIN `{{ params.project_id }}.dl.arara_locations` l ON a.location_id = l.id
  AND a.region = l.region
LEFT JOIN `{{ params.project_id }}.dl.arara_cities` c ON l.city_id = c.id
  AND l.region = c.region
LEFT JOIN applicant_fields af ON af.region = a.region
  AND af.applicant_id = a.applicant_id
LEFT JOIN tiers t ON t.region = a.region
  AND t.applicant_id = a.applicant_id
  AND t.workflow_id = a.workflow_id
LEFT JOIN urls u ON u.url_id = a.url_id
