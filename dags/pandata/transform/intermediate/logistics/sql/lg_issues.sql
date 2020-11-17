WITH issues_agg_actions AS (
  SELECT
    issues.issue_id,
    issues.country_code,
    ARRAY_AGG(
      STRUCT(
        actions.action_id AS id,
        actions.action AS name,
        actions.status,
        actions.time_to_trigger_minutes AS time_to_trigger_in_minutes,
        actions.created_at AS created_at_utc,
        actions.updated_at AS updated_at_utc,
        actions.conditions
      )
    ) AS actions
  FROM `fulfillment-dwh-production.curated_data_shared.issues` AS issues
  CROSS JOIN UNNEST (issues.actions) AS actions
  GROUP BY
    issues.issue_id,
    issues.country_code
)

SELECT
  issues.issue_id || '_' || issues.country_code AS uuid,
  issues.issue_id AS id,
  issues.delivery_id AS lg_delivery_id,
  `{project_id}`.pandata_intermediate.LG_UUID(issues.delivery_id, issues.country_code) AS lg_delivery_uuid,
  issues.rider_id AS lg_rider_id,
  `{project_id}`.pandata_intermediate.LG_UUID(issues.rider_id, issues.country_code) AS lg_rider_uuid,
  `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(issues.country_code, issues.city_id) AS lg_city_id,
  `{project_id}`.pandata_intermediate.LG_UUID(
    `{project_id}`.pandata_intermediate.EXCLUDE_T2_CITY(issues.country_code, issues.city_id),
    issues.country_code
  ) AS lg_city_uuid,
  issues.zone_id AS lg_zone_id,
  `{project_id}`.pandata_intermediate.LG_UUID(issues.zone_id, issues.country_code) AS lg_zone_uuid,
  `{project_id}`.pandata_intermediate.EXCLUDE_T2(issues.country_code) AS country_code,

  issues.issue_name AS name,
  issues.notes,

  issues.show_on_watchlist AS is_shown_on_watchlist,

  issues.issue_type AS type,
  issues.issue_category AS category,

  issues.timezone,
  issues.picked_at AS picked_at_utc,
  issues.picked_by AS picked_by_utc,
  issues.dismissed_at AS dismissed_at_utc,
  issues.resolved_at AS resolved_at_utc,
  issues.created_date AS created_date_utc,
  issues.created_at AS created_at_utc,
  issues.updated_at AS updated_at_utc,
  issues_agg_actions.actions
FROM `fulfillment-dwh-production.curated_data_shared.issues` AS issues
LEFT JOIN issues_agg_actions
       ON issues.country_code = issues_agg_actions.country_code
      AND issues.issue_id = issues_agg_actions.issue_id
