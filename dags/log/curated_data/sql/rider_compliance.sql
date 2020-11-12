CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.rider_compliance`
PARTITION BY created_date AS
WITH compliance_violation AS (
  SELECT *
  FROM `{{ params.project_id }}.dl.compliance_violation`
), compliance_rules AS (
  SELECT *
  FROM `{{ params.project_id }}.cl.rider_compliance_rules`
), compliance_actions AS (
  SELECT v.country_code
    , v.id AS violation_id
    , ARRAY_AGG(
        STRUCT(a.started_at
          , a.ended_at
          , a.cancelled_at
          , a.interrupted_at
          , a.state
          , t.type
          , t.schedule
          , t.duration
          , t.name
      )) AS actions
  FROM compliance_violation v
  LEFT JOIN `{{ params.project_id }}.dl.compliance_action` a ON v.country_code = a.country_code
  AND v.id = a.violation_id
  LEFT JOIN compliance_rules r ON v.rule_id = r.rule_id
    AND v.country_code = r.country_code
  LEFT JOIN UNNEST(actions_template) t ON a.action_template_id = t.id
  GROUP BY 1, 2
), rules AS (
  SELECT v.country_code
    , v.id AS violation_id
    , r.timezone
    , ARRAY_AGG(
        STRUCT(r.rule_id AS id
          , CAST(NULL AS INT64) AS city_id
          , r.violation_type
          , r.rule_type
          , r.contract_type
          , r.violation_duration
          , r.violation_count
          , r.period_duration
      ) ) AS rules
  FROM compliance_violation v
  LEFT JOIN compliance_rules r ON v.rule_id = r.rule_id
    AND v.country_code = r.country_code
  GROUP BY 1, 2, 3
)
SELECT v.country_code
  , v.employee_id AS rider_id
  , v.employee_name AS rider_name
  , DATE(v.process_at, r.timezone) AS created_date
  , r.timezone
  , ARRAY_AGG(
      STRUCT(v.id
        , CAST(NULL AS STRING) AS city_name
        , v.created_at
        , v.process_at
        , v.processed_at
        , v.cancelled_at
        , v.state
        , a.actions
        , r.rules
    ) ORDER BY v.rule_id, v.process_at ASC) AS violations
FROM compliance_violation v
LEFT JOIN compliance_actions a ON v.country_code = a.country_code
  AND v.id = a.violation_id
LEFT JOIN rules r ON v.country_code = r.country_code
 AND v.id = r.violation_id
GROUP BY 1, 2, 3, 4, 5
