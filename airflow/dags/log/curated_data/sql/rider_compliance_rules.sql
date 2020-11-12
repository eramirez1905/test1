CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.rider_compliance_rules`
PARTITION BY created_date AS
WITH countries AS (
  SELECT country_code
    , ci.id AS city_id
    , ci.name AS city_name
    , ci.timezone AS timezone
  FROM `{{ params.project_id }}.cl.countries` co
  LEFT JOIN UNNEST(co.cities) ci
), rules AS (
  SELECT *
  FROM `{{ params.project_id }}.dl.compliance_rule`
), cities_dataset AS (
  SELECT r.country_code
     , r.id AS rule_id
     , id AS city_id
  FROM rules r
  LEFT JOIN UNNEST(city_ids) id
), cities_cleaned AS (
  SELECT c.country_code
     , c.rule_id
     , ARRAY_AGG(
         STRUCT(c.city_id AS id
           , co.city_name AS name
           , co.timezone
       )) AS cities
  FROM cities_dataset c
  LEFT JOIN countries co ON c.country_code = co.country_code
    AND co.city_id = c.city_id
  GROUP BY 1, 2
), action_templates_dataset AS (
  SELECT r.country_code
    , r.id AS rule_id
    , ai AS action_template_id
  FROM rules r
  LEFT JOIN UNNEST(action_template_ids) ai
), action_templates_cleaned AS (
  SELECT ad.country_code
    , ad.rule_id
    , ARRAY_AGG(
        STRUCT(ad.action_template_id AS id
          , t.action_type AS type
          , t.duration
          , t.schedule
          , t.notification_text
          , t.name
      )) AS actions_template
  FROM action_templates_dataset ad
  LEFT JOIN `{{ params.project_id }}.dl.compliance_action_template` t ON ad.country_code = t.country_code
    AND ad.action_template_id = t.id
  GROUP BY 1, 2
), country_timezone AS (
  SELECT country_code
    , country_name
    -- taking one timezone per country to use in the final table when riders have no contract
    , (SELECT timezone FROM UNNEST (cities) ci LIMIT 1) AS country_timezone
  FROM `{{ params.project_id }}.cl.countries`
), combined_rules AS (
  SELECT r.region
    , r.country_code
    , r.created_date
    , r.id AS rule_id
    , r.name AS rule_name
    , r.violation_type
    , r.contract_type
    , r.created_at
    , CAST(NULL AS INT64) AS city_id
    , CAST(NULL AS STRING) AS city_name
    , ct.country_timezone AS timezone
    , r.active
    , r.violation_duration
    , r.violation_category
    , r.rule_type
    , r.violation_count
    , r.period_duration
    , ARRAY_AGG(
       STRUCT(r.employees_ids AS rider_ids
         , r.batch_numbers
         , ci.cities
      )) AS applies_to
 FROM  rules r
 LEFT JOIN cities_cleaned ci ON r.country_code = ci.country_code
   AND r.id = ci.rule_id
 LEFT JOIN country_timezone ct ON r.country_code = ct.country_code
 GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
)

SELECT r.region
  , r.country_code
  , r.created_date
  , r.rule_id
  , r.rule_name
  , r.violation_type
  , r.contract_type
  , r.created_at
  , r.city_id
  , r.city_name
  , r.timezone
  , r.active
  , r.violation_duration
  , r.violation_category
  , r.rule_type
  , r.violation_count
  , r.period_duration
  , r.applies_to
  , a.actions_template
FROM combined_rules r
LEFT JOIN action_templates_cleaned a USING (country_code, rule_id)
