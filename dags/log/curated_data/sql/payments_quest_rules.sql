CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.payments_quest_rules`
PARTITION BY created_date AS
SELECT r.country_code
  , r.created_date
  , r.created_at
  , r.updated_at
  , r.id
  , r.type
  , r.active
  , r.name
  , r.created_by
  , r.status
  , r.duration
  , r.start_date
  , r.end_date
  , r.start_time
  , r.end_time
  , r.acceptance_rate
  , r.no_show_limit
  , r.sub_type
  , r.pay_below_threshold AS is_pay_below_threshold
  , r.negative AS is_negative
  , ARRAY_AGG(
      STRUCT(r.contract_types
        , r.contract_ids
        , r.employee_ids AS rider_ids
        , r.vehicle_types
        , r.job_titles
        , r.starting_point_ids
        , r.days_of_week
        , r.city_ids
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
FROM `{{ params.project_id }}.dl.rooster_payments_quest_payment_rule` r
LEFT JOIN `{{ params.project_id }}.dl.rooster_payments_goal` g ON r.country_code = g.country_code
  AND r.id = g.payment_rule_id
  AND g.payment_type = 'QUEST'
GROUP BY  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20
