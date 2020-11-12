CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.issue_automation_actions_conditions` AS
WITH possible_automation AS (
  -- find all combinations of issue types and actions that can be automated.
  SELECT c.country_code
    , issue_type
    , a.type AS action
  FROM `{{ params.project_id }}.cl._issue_rules_templates`
  LEFT JOIN UNNEST(actions) a
  CROSS JOIN `{{ params.project_id }}.cl.countries` c
  WHERE c.country_code IS NOT NULL
), implemented_base AS (
  -- find all actions and conditions actually implemented by country.
  SELECT id AS action_id
    , rt.country_code
    , rt.issue_type
    , a.type AS action
    , CASE
        WHEN a.type = 'put_courier_on_break'
          THEN CONCAT(arga.value, ' minute break')
        WHEN CHAR_LENGTH(arga.value) = 5
          THEN NULL
        ELSE arga.value
      END AS action_value
    , IFNULL(CAST(ta.value AS INT64), 0) AS time_to_trigger_minutes
    , rt.updated_at
    , IFNULL(CONCAT(lower(co.type), ': ', lower(coarg.value)), 'No Condition') AS conditions_concat
  FROM `{{ params.project_id }}.cl._issue_rules_templates` rt
  LEFT JOIN UNNEST(actions) a
  LEFT JOIN UNNEST(a.args) arga
  LEFT JOIN UNNEST(trigger.args) ta
  LEFT JOIN UNNEST(conditions) co
  LEFT JOIN UNNEST(co.args) coarg
), implemented_actions AS (
  SELECT action_id
    , country_code
    , issue_type
    , time_to_trigger_minutes
    -- combine conditions into one field for better visualization.
    , ARRAY_TO_STRING(ARRAY_AGG(DISTINCT conditions_concat ORDER BY conditions_concat DESC), ' / ') AS conditions_value
    , action
    , MAX(action_value) AS action_value
  FROM implemented_base
  GROUP BY 1,2,3,4,6
)
SELECT DISTINCT pa.country_code
  , co.country_name
  , pa.issue_type
  , pa.action
  , ia.action_value
  , ia.action IS NOT NULL AS is_implemented
  , REGEXP_REPLACE(ia.conditions_value, '_', ' ') AS conditions_value
  , ia.time_to_trigger_minutes
FROM possible_automation pa
LEFT JOIN implemented_actions ia ON pa.country_code = ia.country_code
  AND pa.issue_type = ia.issue_type
  AND pa.action = ia.action
LEFT JOIN `{{ params.project_id }}.cl.countries` co ON co.country_code = pa.country_code
-- removing darkstore pickers to not impact utr as they do not have orders, only working hours
WHERE pa.country_code NOT LIKE '%dp%'

