CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.riders` AS
WITH batch_history AS (
  SELECT country_code
    , employee_id
    , created_at AS active_from
    , LEAD(created_at) OVER (PARTITION BY country_code, employee_id ORDER BY created_at) AS active_until
    , batch_number
    , id
  FROM `{{ params.project_id }}.dl.rooster_batch_history`
), batches AS (
  SELECT country_code
    , employee_id AS rider_id
    , ARRAY_AGG(
        STRUCT(id
          , batch_number AS number
          , active_from
          , COALESCE(active_until, '{{next_execution_date}}') AS active_until
        )
      ) AS batches
  FROM batch_history
  GROUP BY 1, 2
), employees AS (
  SELECT *
  FROM `{{ params.project_id }}.cl._rooster_employees`
), contract_interval_rules AS (
  SELECT ec.country_code
    , cir.contract_id
    , ARRAY_AGG(
        STRUCT(
          cir.id
          , cir.interval_rule_type
          , cir.amount
          , cir.interval_type
          , cir.interval_period
          , cir.created_at
          , cir.updated_at
        )
      ) AS interval_rules
  FROM `{{ params.project_id }}.dl.rooster_contract` ec
  LEFT JOIN `{{ params.project_id }}.dl.rooster_contract_interval_rule` cir ON cir.country_code = ec.country_code
    AND cir.contract_id = ec.id
  GROUP BY 1,2
), contracts AS (
  SELECT e.country_code
    , e.rider_id
    , e.rider_name
    , e.email
    , e.phone_number
    , e.reporting_to
    , e.batch_number
    , e.created_at
    , MAX(GREATEST(e.updated_at, IFNULL(ec.updated_at, '0001-01-01'), IFNULL(c.updated_at, '0001-01-01'))) AS updated_at
    , ARRAY_AGG(
        STRUCT(ec.contract_id AS id
          , c.type
          , ec.status
          , ec.city_id
          , c.name
          , ec.job_title
          , ec.start_at
          , ec.end_at
          , ec.termination_reason
          , ec.termination_type
          , ec.created_at
          , ec.updated_at
          , cir.interval_rules
        )
      ) AS contracts
  FROM `employees` e
  LEFT JOIN `{{ params.project_id }}.dl.rooster_employee_contract` ec ON e.country_code = ec.country_code
    AND e.rider_id = ec.employee_id
  LEFT JOIN `{{ params.project_id }}.dl.rooster_contract` c ON ec.country_code = c.country_code
    AND ec.contract_id = c.id
  LEFT JOIN contract_interval_rules cir ON cir.country_code = c.country_code
    AND cir.contract_id = c.id
  GROUP BY 1,2,3,4,5,6,7,8
), absences_history AS (
  SELECT a.country_code
    , a.employee_id
    , MAX(a.updated_at) AS updated_at
    , ARRAY_AGG(
        STRUCT(a.id
          , a.start_at
          , a.end_at
          , a.type AS reason
          , a.paid AS is_paid
          , a.status
          , a.updated_by AS user_id
          , a.created_at
          , a.updated_at
          , a.comment
          -- the following REGEX_EXTRACT function is needed to get out the int from the string. The string format is {123456}
          , SAFE_CAST(REGEXP_EXTRACT(attachment, r'[0-9]+') AS INT64) AS violation_id
        )
      ) AS absences_history
  FROM `{{ params.project_id }}.dl.rooster_absence` a
  GROUP BY 1, 2
), hurrier_couriers AS (
  SELECT country_code
    , user_id
    , ARRAY_AGG(id) AS hurrier_courier_ids
  FROM `{{ params.project_id }}.dl.hurrier_couriers`
  GROUP BY 1, 2
), employee_vehicles AS (
  SELECT ev.country_code
    , ev.employee_id
    , MAX(ev.updated_at) AS updated_at
    , ARRAY_AGG(
        STRUCT(
          ev.vehicle_type_id
          , ev.created_at
          , ev.updated_at
          , ev.created_by
          , ev.updated_by
          , vt.name
          , vt.profile
        )
      ) AS rider_vehicles
  FROM `{{ params.project_id }}.dl.rooster_employee_vehicle` ev
  LEFT JOIN `{{ params.project_id }}.dl.rooster_vehicle_type` vt ON ev.country_code = vt.country_code
    AND ev.vehicle_type_id = vt.id
  GROUP BY 1, 2
), rider_feedbacks AS (
  SELECT f.country_code
    , c.user_id AS rider_id
    , MAX(f.updated_at) AS updated_at
    , ARRAY_AGG(
        STRUCT(f.user_id
          , f.order_id
          , f.created_at
          , f.reason
          , f.comment
        )
      ) AS rider_feedbacks
  FROM `{{ params.project_id }}.dl.hurrier_courier_feedbacks` f
  LEFT JOIN `{{ params.project_id }}.dl.hurrier_couriers` c ON f.country_code = c.country_code
    AND f.courier_id = c.id
  GROUP BY 1, 2
), nps_answers AS (
  SELECT np.country_code
    , np.rider_id
    , ARRAY_AGG(
        STRUCT( survey_id
        , survey_create_datetime
        , survey_start_datetime
        , survey_end_datetime
        , created_date
        , source_type
        , response_id
        , nps_score
        , nps_reason
        , rider_type
        , is_finished
        , details
        )
    ) AS nps
FROM `{{ params.project_id }}.cl._nps_rider_survey_response` np
GROUP BY 1, 2
)
SELECT e.country_code
    , e.rider_id
    , h.hurrier_courier_ids
    , e.rider_name
    , e.email
    , e.phone_number
    , e.reporting_to
    , e.batch_number
    , e.created_at
    , e.birth_date
    , ARRAY(SELECT AS STRUCT c.* FROM UNNEST(contracts) AS c ORDER BY c.status DESC, c.start_at, c.end_at) AS contracts
    , ARRAY(SELECT AS STRUCT b.id, b.number, b.active_from, b.active_until FROM UNNEST(batches) AS b ORDER BY b.active_from) AS batches
    , ARRAY(SELECT AS STRUCT cf.* FROM UNNEST(custom_fields) AS cf ORDER BY cf.name) AS custom_fields
    , ARRAY(SELECT AS STRUCT a.* FROM UNNEST(absences_history) AS a ORDER BY a.start_at) AS absences_history
    , ARRAY(SELECT AS STRUCT ev.* FROM UNNEST(rider_vehicles) AS ev ORDER BY e.created_at) AS rider_vehicles
    , starting_points
    , ARRAY(SELECT AS STRUCT rf.* FROM UNNEST(rider_feedbacks) AS rf ORDER BY e.created_at) AS rider_feedbacks
    , ARRAY(SELECT AS STRUCT nps.* FROM UNNEST(nps) AS nps) AS nps
    , GREATEST(e.updated_at, IFNULL(c.updated_at, '0001-01-01'), IFNULL(a.updated_at, '0001-01-01'), IFNULL(ev.updated_at, '0001-01-01'), IFNULL(sp.updated_at, '0001-01-01'), IFNULL(rf.updated_at, '0001-01-01')) AS updated_at
FROM employees e
LEFT JOIN contracts c ON e.country_code = c.country_code
  AND e.rider_id = c.rider_id
LEFT JOIN hurrier_couriers h ON e.country_code = h.country_code
  AND e.rider_id = h.user_id
LEFT JOIN batches ba ON e.country_code = ba.country_code
  AND e.rider_id = ba.rider_id
LEFT JOIN absences_history a ON e.country_code = a.country_code
  AND e.rider_id = a.employee_id
LEFT JOIN employee_vehicles ev ON ev.country_code = e.country_code
  AND ev.employee_id = e.rider_id
LEFT JOIN `{{ params.project_id }}.cl._employee_starting_points` sp ON e.country_code = sp.country_code
  AND e.rider_id = sp.employee_id
LEFT JOIN rider_feedbacks rf ON rf.country_code = e.country_code
  AND rf.rider_id = e.rider_id
LEFT JOIN nps_answers nps_a ON e.country_code = nps_a.country_code
  AND e.rider_id = nps_a.rider_id
