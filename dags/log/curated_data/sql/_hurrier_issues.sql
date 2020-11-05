{% include 'issues_functions.sql' %}

CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._hurrier_issues`
PARTITION BY created_date
CLUSTER BY country_code, delivery_id, issue_id, issue_type AS
WITH deliveries AS ( -- getting city and timezone from orders
  SELECT o.country_code
    , o.order_id
    , d.id
    , o.city_id
    , o.zone_id
    , o.timezone
    , d.created_at
    , d.rider_id
  FROM `{{ params.project_id }}.cl.orders` o
  LEFT JOIN UNNEST(deliveries) d
), rider_timezone AS ( -- getting the timezone of the riders by day they worked
  SELECT DISTINCT r.rider_id
    , r.country_code
    , c.city_id
    , ci.name AS city_name
    , ci.timezone
    , c.start_at
    , c.end_at
  FROM `{{ params.project_id }}.cl.riders` r
  LEFT JOIN UNNEST(contracts) c
  LEFT JOIN `{{ params.project_id }}.cl.countries` co ON co.country_code = r.country_code
  LEFT JOIN UNNEST(co.cities) ci ON ci.id = c.city_id
  WHERE c.status = 'VALID'
), country_timezone AS (
  SELECT c.country_code
    , c.country_name
    -- taking one timezone per country to use in the final table when riders have no contract
    , (SELECT timezone FROM UNNEST (cities) ci LIMIT 1) AS timezone
  FROM `{{ params.project_id }}.cl.countries` c
), base_actions_conditions AS ( -- extract all actions, conditions and triggers from JSON format
  SELECT issue_id
    , id AS action_id
    , country_code
    , status
    , parse_json(JSON_EXTRACT(actions, '$.data')) AS actions
    , parse_trigger_json(trigger) AS trigger
    , created_at
    , updated_at
    , parse_json(JSON_EXTRACT(conditions, '$.data')) AS conditions
  FROM `{{ params.project_id }}.dl.hurrier_issue_rules` hir
  WHERE status = 'completed'
), actions_conditions AS ( -- make conditions an array of actions.
  SELECT b.issue_id
    , CAST(b.action_id AS STRING) AS action_id
    , b.country_code
    , b.status
    , IF(b.trigger.type = 'on_issue_creation', 0, CAST(arg.value AS INT64)) AS time_to_trigger_minutes
    , a.type AS action
    , arga.value AS action_value
    , b.created_at
    , b.updated_at
    , COUNT(a.type) OVER (PARTITION BY country_code, issue_id) AS action_count --needed later to create solved_by logic
    , ARRAY_AGG(STRUCT(c.type AS type, argc.value AS value) ORDER BY c.type, argc.value) AS conditions
  FROM base_actions_conditions b
  LEFT JOIN UNNEST(actions) a
  LEFT JOIN UNNEST(a.args) arga
  LEFT JOIN UNNEST(trigger.args) arg
  LEFT JOIN UNNEST(conditions) c
  LEFT JOIN UNNEST(c.args) AS argc
  GROUP BY 1,2,3,4,5,6,7,8,9
), watchlist_only AS (
  SELECT country_code
    , issue_id
    , 'self resolved' AS self_solved
  FROM actions_conditions
  WHERE action_count = 1
    AND action = 'show_on_issue_watchlist'
), audit AS (
  SELECT l.country_code
    , hurrier.resolve_issue.issue_id AS issue_id
    , IF(l.user_id IS NOT NULL, 'dispatcher', NULL) AS dispatcher_solved
  FROM `{{ params.project_id }}.cl.audit_logs` l
  WHERE l.action IN ('resolve_issue')
), action_array AS ( -- make actions an array of issues. Grouping by only PK
  SELECT issue_id
    , country_code
    , action_count
    , ARRAY_AGG(
        STRUCT(action_id
          , time_to_trigger_minutes
          , action
          , action_value
          , status
          , created_at
          , updated_at
          , conditions
        ) ORDER BY created_at
      ) AS actions
    , TRUE AS is_automated
  FROM actions_conditions
  GROUP BY 1,2,3
 ), cleaned_dataset AS (
  SELECT i.country_code
    , CAST(i.created_at AS DATE) AS created_date
    , i.id AS issue_id
    , i.external_id
    , i.delivery_id AS delivery_id
    , c.user_id AS rider_id
    , COALESCE(c.ghost, FALSE) AS is_ghost_rider
    , COALESCE(d.city_id, rt.city_id) AS city_id
    , d.zone_id
    , COALESCE(d.timezone, rt.timezone, ct.timezone) AS timezone
    , i.type AS issue_type
    , i.category AS issue_category
    , COALESCE(ia.name, CONCAT(i.type, ' ', i.category)) AS issue_name
    , i.notes
    , aa.actions
    , i.picked_at
    , i.picked_by
    , i.dismissed_at
    , i.resolved_at
    , CASE
        WHEN audit.dispatcher_solved IS NOT NULL
          THEN audit.dispatcher_solved
        WHEN wo.self_solved IS NOT NULL
          THEN wo.self_solved
        WHEN aa.is_automated IS TRUE AND resolved_at IS NOT NULL
          THEN 'ICE'
        ELSE NULL
      END AS resolved_by
    , i.show_on_watchlist
    , i.created_at
    , i.updated_at
    , COALESCE(aa.is_automated, FALSE) AS is_automated
    -- the join with rider_timezones is generating duplicates, as rider can have multiple contracts overlapping
    -- the ROW_NUMBER is taking the one with the latest contract end. This is hotfix as we can't take city and zone
    -- somewhere else reliably
    , ROW_NUMBER() OVER (PARTITION BY i.country_code, i.id ORDER BY rt.end_at DESC) AS _row_number
  FROM `{{ params.project_id }}.dl.hurrier_issues` i
  LEFT JOIN deliveries d ON d.country_code = i.country_code
    AND d.id = i.delivery_id
  LEFT JOIN `{{ params.project_id }}.dl.hurrier_couriers` c ON c.country_code = i.country_code
    AND i.courier_id = c.id
  LEFT JOIN action_array aa ON aa.country_code = i.country_code
    AND aa.issue_id = i.id
  LEFT JOIN rider_timezone rt on rt.country_code = i.country_code
    AND rt.rider_id = c.user_id
    AND i.created_at BETWEEN rt.start_at AND rt.end_at
    -- enable fallback join only if city_id is null in deliveries
    AND d.city_id IS NULL
  LEFT JOIN `{{ params.project_id }}.cl._issues_automated_naming` ia ON ia.type = i.type
    AND ia.category = i.category
  LEFT JOIN watchlist_only wo ON wo.country_code = i.country_code
    AND wo.issue_id = i.id
  LEFT JOIN audit ON audit.country_code = i.country_code
    AND audit.issue_id = i.id
  LEFT JOIN country_timezone ct ON i.country_code = ct.country_code
)
SELECT *
FROM cleaned_dataset
WHERE _row_number = 1 OR _row_number IS NULL
