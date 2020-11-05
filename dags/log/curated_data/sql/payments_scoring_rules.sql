CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.payments_scoring_rules`
PARTITION BY created_date AS
SELECT r.country_code
  , r.created_date
  , r.created_at
  , r.updated_at
  , r.id
  , r.type
  , r.name
  , r.paid_unit
  , r.paid_unit_type
  , r.active
  , r.created_by
  , r.city_id
  , r.start_date
  , r.end_date
  , r.status
  , ARRAY_AGG(
      STRUCT(r.contract_types
       , r.contract_ids
       , r.employee_ids AS rider_ids
       , r.city_id
       , r.vehicle_types
       , r.job_titles
       , r.starting_point_ids
       , r.vertical_types
   )) AS applies_to
 , ARRAY_AGG(
     STRUCT(g.type
       , g.created_at
       , g.updated_at
       , g.created_by
       , g.threshold
       , g.amount
   )) AS cost_factors
FROM `{{ params.project_id }}.dl.rooster_payments_scoring_payment_rule` r
LEFT JOIN `{{ params.project_id }}.dl.rooster_payments_goal` g ON r.country_code = g.country_code
  AND r.id = g.payment_rule_id
  AND g.payment_type = 'SCORING'
GROUP BY  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
