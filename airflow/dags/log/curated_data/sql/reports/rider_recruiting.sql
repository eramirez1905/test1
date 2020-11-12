CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.rider_recruiting`
PARTITION BY report_date AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 8 WEEK) AS start_date
   , DATE_SUB('{{ next_ds }}', INTERVAL 6 MONTH) AS second_start_date
), dates AS (
SELECT listed_date
  , FORMAT_DATE('%G-%V', listed_date) AS week_number
FROM (
  SELECT GENERATE_DATE_ARRAY(start_date, '{{ next_ds }}', INTERVAL 7 DAY) AS date_array
  FROM parameters
  ), UNNEST(date_array) listed_date
), entities AS (
  SELECT co.country_code
    -- add a concatenation of all the platform in each country for visualization purposes.
    , ARRAY_TO_STRING(ARRAY_AGG(p.display_name IGNORE NULLS), ' / ') AS entities
  FROM `{{ params.project_id }}.cl.countries` co
  LEFT JOIN UNNEST (co.platforms) p
  -- remove legacy display name from report, as there is no data under it since 2017, however it may cause confusion to the user.
  WHERE p.display_name NOT IN ('FD - Bahrain')
  -- removing darkstore pickers to not impact utr as they do not have orders, only working hours
    AND co.country_code NOT LIKE '%dp%'
  GROUP BY  1
), rider_shifts AS (
  SELECT country_code
    , rider_id
    , MIN(DATE(DATETIME(actual_start_at, timezone))) AS first_shift
    , MAX(DATE(DATETIME(actual_start_at, timezone))) AS last_shift
    , MIN(FORMAT_DATE('%G-%V', DATE(DATETIME(actual_start_at, timezone)))) AS first_shift_week
    , MAX(FORMAT_DATE('%G-%V', DATE(DATETIME(actual_start_at, timezone)))) AS last_shift_week
    , ARRAY_AGG(DISTINCT DATE(DATETIME(actual_start_at, timezone))) AS array_shift_day
    , ARRAY_AGG(DISTINCT FORMAT_DATE('%G-%V', DATE(DATETIME(actual_start_at, timezone)))) AS array_shift_week
  FROM `{{ params.project_id }}.cl.shifts`
  WHERE created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 36 MONTH)
    AND shift_state = 'EVALUATED'
    -- removing darkstore pickers to not impact utr as they do not have orders, only working hours
    AND country_code NOT LIKE '%dp%'
  GROUP BY 1, 2
), batches AS (
  SELECT country_code
    , rider_id
    , b.number AS batch_number
    , b.active_from
    , b.active_until
  FROM `{{ params.project_id }}.cl.riders` r
  LEFT JOIN UNNEST(batches) b
  -- removing darkstore pickers to not impact utr as they do not have orders, only working hours
  WHERE country_code NOT LIKE '%dp%'
), contracts AS (
  SELECT r1.country_code
    , r1.rider_id
    , r1.rider_name
    , r1.email
    , r1.batch_number
    , r1.created_at AS rider_created_at
    , r1.reporting_to AS captain_id
    , r2.rider_name AS captain_name
    , c.id
    , c.city_id
    , c.type
    , c.name
    , c.start_at
    , c.end_at
    , CASE
        WHEN c.end_at >= CURRENT_TIMESTAMP
          THEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP, c.start_at, DAY) / 7
        ELSE TIMESTAMP_DIFF(c.end_at, c.start_at, DAY) / 7
      END AS contract_duration_in_weeks
    , c.created_at
    , CASE
        WHEN c.name IS NULL
          THEN 'inactive' ELSE 'active'
        END AS rider_contract_status  ----- what does this mean ?
    , ROW_NUMBER() OVER(PARTITION BY r1.country_code, r1.rider_id ORDER BY start_at DESC, end_at DESC) AS rank_contract
  FROM `{{ params.project_id }}.cl.riders` r1
  LEFT JOIN `{{ params.project_id }}.cl.riders` r2 ON r1.country_code = r2.country_code
   AND r1.reporting_to = r2.rider_id
  LEFT JOIN UNNEST(r1.contracts) c ON DATE(c.start_at) <= '{{ next_ds }}' ---- consider only contracts which have already started
  WHERE c.status = 'VALID'
  -- removing darkstore pickers to not impact utr as they do not have orders, only working hours
   AND r1.country_code NOT LIKE '%dp%'
), shifts AS (
  SELECT country_code
    , rider_id
    , FORMAT_DATE('%G-%V', DATE(DATETIME(s.actual_start_at, s.timezone))) AS report_week
    , SUM(actual_working_time / 3600) AS hours_worked
  FROM `{{ params.project_id }}.cl.shifts` s
  WHERE s.created_date >= (SELECT start_date FROM parameters)
    AND shift_state IN ('EVALUATED')
    -- removing darkstore pickers to not impact utr as they do not have orders, only working hours
    AND s.country_code NOT LIKE '%dp%'
  GROUP BY 1, 2, 3
), tenure AS (
  SELECT country_code
    , rider_id
    , ROUND(SUM(c.contract_duration_in_weeks), 1) AS tenure_in_weeks
    , ROUND(SUM(c.contract_duration_in_weeks) / 52, 1) AS tenure_in_years
  FROM contracts c
  GROUP BY 1, 2
)
  SELECT d.listed_date AS report_date
    , d.week_number AS report_week
    , c.country_code
    , co.country_name
    , en.entities
    , IF(ARRAY_LENGTH(co.platforms) = 1, co.platforms[OFFSET(0)].display_name, en.entities) AS entity
    , c.city_id
    , ci.name AS city_name
    --hotfix to prevent under count that was happening inside tableau due to use of count distinct
    , CONCAT(CAST(co.country_code AS STRING), "", CAST(c.rider_id AS STRING)) AS rider_id
    , c.rider_name
    , c.email
    , c.batch_number AS current_batch_number
    , ba.batch_number
    , c.type AS contract_type
    , c.name AS contract_name
    , DATE(DATETIME(c.start_at, ci.timezone)) AS contract_start_date_local
    , DATE(DATETIME(c.end_at, ci.timezone)) AS contract_end_date_local
    , FORMAT_DATE('%G-%V', DATE(DATETIME(c.start_at, ci.timezone))) AS contract_start_week
    , FORMAT_DATE('%G-%V', DATE(DATETIME(c.end_at, ci.timezone))) AS contract_end_week
    , DATE(DATETIME(c.created_at, ci.timezone)) AS contract_creation_date_local
    , DATE(DATETIME(c.rider_created_at, ci.timezone)) AS hiring_date_local
    , FORMAT_DATE('%G-%V', DATE(DATETIME(c.rider_created_at, ci.timezone))) AS hiring_week
    , first_shift
    , first_shift_week
    , last_shift
    , last_shift_week
    , last_shift_week < FORMAT_DATE('%G-%V', DATE_SUB('{{ next_ds }}', INTERVAL 9 WEEK)) AS no_work_last_9_weeks
    , last_shift_week < FORMAT_DATE('%G-%V', DATE_SUB('{{ next_ds }}', INTERVAL 8 WEEK)) AS no_work_last_8_weeks
    , last_shift_week < FORMAT_DATE('%G-%V', DATE_SUB('{{ next_ds }}', INTERVAL 4 WEEK)) AS no_work_last_4_weeks
    , last_shift_week < FORMAT_DATE('%G-%V', DATE_SUB('{{ next_ds }}', INTERVAL 3 WEEK)) AS no_work_last_3_weeks
    , last_shift_week < FORMAT_DATE('%G-%V', DATE_SUB('{{ next_ds }}', INTERVAL 2 WEEK)) AS no_work_last_2_weeks
    , last_shift_week < FORMAT_DATE('%G-%V', DATE_SUB('{{ next_ds }}', INTERVAL 1 WEEK)) AS no_work_last_week
    , last_shift_week < FORMAT_DATE('%G-%V', '{{ next_ds }}') AS no_work_this_week
    , c.rider_contract_status
    , c.captain_name
    , tenure_in_weeks
    , tenure_in_years
    , CASE
        WHEN first_shift IS NOT NULL AND first_shift_week <= FORMAT_DATE('%G-%V', d.listed_date)
          THEN TRUE
        ELSE FALSE
      END AS hired_active
    , CASE
        WHEN FORMAT_DATE('%G-%V', d.listed_date) IN UNNEST(rs.array_shift_week)
          THEN TRUE
        ELSE FALSE
      END AS is_active_current_week
    , CASE
        WHEN FORMAT_DATE('%G-%V', d.listed_date) IN UNNEST(rs.array_shift_week)
          OR FORMAT_DATE('%G-%V', DATE_SUB(d.listed_date, INTERVAL 1 WEEK)) IN UNNEST(rs.array_shift_week)
          OR FORMAT_DATE('%G-%V', DATE_SUB(d.listed_date, INTERVAL 2 WEEK)) IN UNNEST(rs.array_shift_week)
          OR FORMAT_DATE('%G-%V', DATE_SUB(d.listed_date, INTERVAL 3 WEEK)) IN UNNEST(rs.array_shift_week)
          THEN TRUE
        ELSE FALSE
      END AS is_active_last_4_week
    , CASE
        WHEN FORMAT_DATE('%G-%V', d.listed_date) IN UNNEST(rs.array_shift_week)
          OR FORMAT_DATE('%G-%V', DATE_SUB(d.listed_date, INTERVAL 1 WEEK)) IN UNNEST(rs.array_shift_week)
          OR FORMAT_DATE('%G-%V', DATE_SUB(d.listed_date, INTERVAL 2 WEEK)) IN UNNEST(rs.array_shift_week)
          OR FORMAT_DATE('%G-%V', DATE_SUB(d.listed_date, INTERVAL 3 WEEK)) IN UNNEST(rs.array_shift_week)
          OR FORMAT_DATE('%G-%V', DATE_SUB(d.listed_date, INTERVAL 4 WEEK)) IN UNNEST(rs.array_shift_week)
          THEN TRUE
        ELSE FALSE
      END AS is_active_last_5_week
    , CASE
        WHEN FORMAT_DATE('%G-%V', d.listed_date) = first_shift_week
          THEN TRUE
        ELSE FALSE
      END AS is_new_rider_current_week
    , CASE
        WHEN FORMAT_DATE('%G-%V', d.listed_date) = first_shift_week
          OR FORMAT_DATE('%G-%V', DATE_SUB(d.listed_date, INTERVAL 1 WEEK)) = first_shift_week
          OR FORMAT_DATE('%G-%V', DATE_SUB(d.listed_date, INTERVAL 2 WEEK)) = first_shift_week
          OR FORMAT_DATE('%G-%V', DATE_SUB(d.listed_date, INTERVAL 3 WEEK)) = first_shift_week
          THEN TRUE
        ELSE FALSE
      END AS is_new_rider_last_4_weeks
    , CASE
        WHEN FORMAT_DATE('%G-%V', d.listed_date) IN UNNEST(rs.array_shift_week)
          AND NOT FORMAT_DATE('%G-%V', DATE_SUB(d.listed_date, INTERVAL 1 WEEK)) IN UNNEST(rs.array_shift_week)
          AND NOT FORMAT_DATE('%G-%V', DATE_SUB(d.listed_date, INTERVAL 2 WEEK)) IN UNNEST(rs.array_shift_week)
          AND NOT FORMAT_DATE('%G-%V', DATE_SUB(d.listed_date, INTERVAL 3 WEEK)) IN UNNEST(rs.array_shift_week)
          AND NOT FORMAT_DATE('%G-%V', DATE_SUB(d.listed_date, INTERVAL 4 WEEK)) IN UNNEST(rs.array_shift_week)
          AND first_shift_week <= FORMAT_DATE('%G-%V', DATE_SUB(d.listed_date, INTERVAL 2 WEEK))
          THEN TRUE
        ELSE FALSE
      END AS reactivated_after_a_month
     , CASE
        WHEN NOT FORMAT_DATE('%G-%V', d.listed_date) IN UNNEST(rs.array_shift_week)
          AND NOT FORMAT_DATE('%G-%V', DATE_SUB(d.listed_date, INTERVAL 1 WEEK)) IN UNNEST(rs.array_shift_week)
          AND NOT FORMAT_DATE('%G-%V', DATE_SUB(d.listed_date, INTERVAL 2 WEEK)) IN UNNEST(rs.array_shift_week)
          AND NOT FORMAT_DATE('%G-%V', DATE_SUB(d.listed_date, INTERVAL 3 WEEK)) IN UNNEST(rs.array_shift_week)
          AND FORMAT_DATE('%G-%V', DATE_SUB(d.listed_date, INTERVAL 4 WEEK)) IN UNNEST(rs.array_shift_week)
          THEN TRUE
        ELSE FALSE
      END AS rider_churned
     , CASE
        WHEN NOT FORMAT_DATE('%G-%V', d.listed_date) IN UNNEST(rs.array_shift_week)
          AND NOT FORMAT_DATE('%G-%V', DATE_SUB(d.listed_date, INTERVAL 1 WEEK)) IN UNNEST(rs.array_shift_week)
          AND NOT FORMAT_DATE('%G-%V', DATE_SUB(d.listed_date, INTERVAL 2 WEEK)) IN UNNEST(rs.array_shift_week)
          AND FORMAT_DATE('%G-%V', DATE_SUB(d.listed_date, INTERVAL 3 WEEK)) IN UNNEST(rs.array_shift_week)
          THEN TRUE
        ELSE FALSE
      END AS churn_risk
    , s.hours_worked
    , CASE
        WHEN DATE_DIFF(first_shift, DATE(DATETIME(c.start_at, ci.timezone)), DAY) < 0
          THEN NULL
        ELSE DATE_DIFF(first_shift, DATE(DATETIME(c.start_at, ci.timezone)), DAY)
      END AS time_to_street
  FROM dates d
  CROSS JOIN contracts c
  LEFT JOIN shifts s ON c.country_code = s.country_code
    AND c.rider_id = s.rider_id
    AND d.week_number = s.report_week
  LEFT JOIN rider_shifts rs ON c.country_code = rs.country_code
    AND c.rider_id = rs.rider_id
  LEFT JOIN tenure t ON c.country_code = t.country_code
    AND c.rider_id = t.rider_id
  LEFT JOIN batches ba ON c.country_code = ba.country_code
    AND c.rider_id = ba.rider_id
    AND TIMESTAMP(d.listed_date) >= ba.active_from
    AND TIMESTAMP(d.listed_date) < ba.active_until  
  LEFT JOIN `{{ params.project_id }}.cl.countries` co ON c.country_code = co.country_code
  LEFT JOIN UNNEST(co.cities) ci ON c.city_id = ci.id
  LEFT JOIN entities en ON c.country_code = en.country_code
  WHERE c.rider_id IS NOT NULL
    AND c.rank_contract = 1
;
