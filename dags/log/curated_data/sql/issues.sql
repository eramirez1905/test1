{% include 'issues_functions.sql' %}

CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.issues`
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
), base_actions_conditions AS (
  SELECT issue_id AS issue_id_external
    , id AS action_id
    , country_code
    , status
    , parse_json(JSON_EXTRACT(actions, '$.data')) AS actions
    , parse_trigger_json(trigger) AS trigger
    , created_at
    , updated_at
    , parse_json(JSON_EXTRACT(conditions, '$.data')) AS conditions
  FROM `{{ params.project_id }}.dl.issue_service_issue_rules` hir
  WHERE status = 'completed'
), actions_conditions AS ( -- make conditions an array of actions.
  SELECT b.issue_id_external
    , CAST(b.action_id AS STRING) AS action_id
    , b.country_code
    , b.status
    , IF(b.trigger.type = 'on_issue_creation', 0, CAST(CAST(arg.value AS NUMERIC) AS INT64)) AS time_to_trigger_minutes
    , a.type AS action
    , arga.value AS action_value
    , b.created_at
    , b.updated_at
    -- needed later to create solved_by logic
    , COUNT(a.type) OVER (PARTITION BY country_code, issue_id_external) AS action_count
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
    , issue_id_external
    , 'self resolved' AS self_solved
  FROM actions_conditions
  WHERE action_count = 1
    AND action = 'show_on_issue_watchlist'
), audit AS (
  SELECT l.country_code
    , hurrier.resolve_issue.external_id
    , IF(l.user_id IS NOT NULL, 'dispatcher', NULL) AS dispatcher_solved
  FROM `{{ params.project_id }}.cl.audit_logs` l
  WHERE l.action IN ('resolve_issue')
    AND hurrier.resolve_issue.external_id IS NOT NULL
), action_array AS ( -- make actions an array of issues. Grouping by only PK
  SELECT issue_id_external
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
      ) ORDER BY time_to_trigger_minutes, created_at
    ) AS actions
    , TRUE AS is_automated
  FROM actions_conditions
  GROUP BY 1,2,3
-- get the watchlist boolean from Hurrier as it is only in hurrier (even for issue service).
), watchlist AS (
  SELECT external_id
    , country_code
    , show_on_watchlist
    , picked_at
    , picked_by
    , dismissed_at
  FROM `{{ params.project_id }}.cl._hurrier_issues`
), issue_service_issues AS (
  SELECT i.country_code
    , i.created_date
    , i.id AS issue_id_external
    , i.delivery_id AS delivery_id
    -- The courier_id in `{{ params.project_id }}.dl.issue_service_issues` is the rooster rider_id.
    , i.courier_id AS rider_id
    , NULL AS is_ghost_rider
    , COALESCE(d.city_id, rt.city_id) AS city_id
    , d.zone_id
    , COALESCE(d.timezone, rt.timezone, ct.timezone) AS timezone
    , i.type AS issue_type
    , NULL AS issue_category
    , COALESCE(ia.name, CONCAT(i.type)) AS issue_name
    , i.notes
    , aa.actions
    , COALESCE(i.picked_at, w.picked_at) AS picked_at
    , COALESCE(i.picked_by, w.picked_by) AS picked_by
    , w.dismissed_at
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
    , COALESCE(i.show_on_watchlist, w.show_on_watchlist) AS show_on_watchlist
    , i.created_at
    , i.updated_at
    , COALESCE(aa.is_automated, FALSE) AS is_automated
    -- the join with rider_timezones is generating duplicates, as rider can have multiple contracts overlapping
    -- the ROW_NUMBER is taking the one with the latest contract end. This is hotfix as we can't take city and zone
    -- somewhere else reliably 
    , ROW_NUMBER() OVER (PARTITION BY i.country_code, i.id ORDER BY rt.end_at DESC) AS _row_number
  FROM `{{ params.project_id }}.dl.issue_service_issues` i
  LEFT JOIN deliveries d ON d.country_code = i.country_code
    AND d.id = i.delivery_id
  LEFT JOIN action_array aa ON aa.country_code = i.country_code
    AND aa.issue_id_external = i.id
  LEFT JOIN rider_timezone rt on rt.country_code = i.country_code
    AND rt.rider_id = i.courier_id
    AND i.created_at BETWEEN rt.start_at AND rt.end_at
    -- enable fallback join only if city_id is null in deliveries
    AND d.city_id IS NULL
  LEFT JOIN `{{ params.project_id }}.cl._issues_automated_naming` ia ON ia.type = i.type
  LEFT JOIN watchlist_only wo ON wo.country_code = i.country_code
    AND wo.issue_id_external = i.id
  LEFT JOIN watchlist w ON w.external_id = i.id
    AND w.country_code = i.country_code
  LEFT JOIN audit ON audit.country_code = i.country_code
    AND audit.external_id = i.id
  LEFT JOIN country_timezone ct ON i.country_code = ct.country_code
), union_sources AS (
 SELECT country_code
   , created_date
   , issue_id_external AS issue_id
   , delivery_id
   , rider_id
   , CAST(NULL AS BOOLEAN) is_ghost_rider
   , city_id
   , zone_id
   , timezone
   , issue_type
   , CAST(issue_category AS STRING) AS issue_category
   , issue_name
   , notes
   , actions
   , picked_at
   , CAST(picked_by AS STRING) AS picked_by
   , dismissed_at
   , resolved_at
   , resolved_by
   , show_on_watchlist
   , created_at
   , updated_at
   , is_automated
   , _row_number
   , 'issue_service' AS _source
 FROM issue_service_issues

 UNION ALL

 SELECT hurr.country_code
   , hurr.created_date
   , CAST(hurr.issue_id AS STRING) AS issue_id
   , hurr.delivery_id
   , hurr.rider_id
   , hurr.is_ghost_rider
   , hurr.city_id
   , hurr.zone_id
   , hurr.timezone
   , hurr.issue_type
   , hurr.issue_category
   , hurr.issue_name
   , hurr.notes
   , hurr.actions
   , hurr.picked_at
   , hurr.picked_by
   , hurr.dismissed_at
   , hurr.resolved_at
   , hurr.resolved_by
   , hurr.show_on_watchlist
   , hurr.created_at
   , hurr.updated_at
   , hurr.is_automated
   , hurr._row_number
   , 'hurrier' AS _source
 FROM `{{ params.project_id }}.cl._hurrier_issues` hurr
 LEFT JOIN issue_service_issues iss_ser ON iss_ser.country_code = hurr.country_code
   AND iss_ser.issue_id_external = hurr.external_id
 -- do not include issues already in issue_service_issues
 WHERE iss_ser.issue_id_external IS NULL
)
SELECT * EXCEPT(_row_number)
FROM union_sources
WHERE _row_number = 1
