{% include 'issues_functions.sql' %}
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._issue_rules_templates` AS
SELECT id
  , country_code
  , created_date
  , issue_type
  , parse_json(JSON_EXTRACT(actions, '$.data')) AS actions
  , parse_trigger_json(trigger) AS trigger
  , parse_json(JSON_EXTRACT(LOWER(conditions), '$.data')) AS conditions
  , created_at
  , updated_at
FROM `{{ params.project_id }}.dl.hurrier_issue_rule_templates`
