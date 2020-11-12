CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.scorecard_report_log_kpis` AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 10 WEEK) AS start_time
    , DATE_SUB('{{ next_ds }}', INTERVAL 1 DAY) AS end_time
), entities AS (
  SELECT co.country_code
    -- add a concatenation of all the platform in each country for visualization purposes.
    , ARRAY_TO_STRING(ARRAY_AGG(p.display_name IGNORE NULLS), ' / ') AS entities
  FROM `{{ params.project_id }}.cl.countries` co
  LEFT JOIN UNNEST (co.platforms) p
  -- remove legacy display name from report, as there is no data under it since 2017, however it may cause confusion to the user.
  WHERE p.display_name NOT IN ('FD - Bahrain')
  GROUP BY  1
), worked_hours AS (
  SELECT s.country_code
--     , IF(ARRAY_LENGTH(co.platforms) = 1, co.platforms[OFFSET(0)].display_name, en.entities) AS entity_display_name
    , s.city_id
    , s.zone_id
    , FORMAT_DATE('%G-%V', e.day) AS report_week_local
    , SUM(e.duration / 3600) AS utr_working_hours
  FROM `{{ params.project_id }}.cl.shifts` s
  LEFT JOIN UNNEST(s.actual_working_time_by_date) e
--   LEFT JOIN `{{ params.project_id }}.cl.countries` co ON s.country_code = co.country_code
--   LEFT JOIN entities en ON s.country_code = en.country_code
  WHERE s.shift_state = 'EVALUATED'
    AND s.created_date >= (SELECT start_time FROM parameters)
    AND e.day BETWEEN (SELECT start_time FROM parameters) AND (SELECT end_time FROM parameters)
  GROUP BY 1, 2, 3, 4--, 5
), utr_deliveries AS (
  SELECT o.country_code
--     , o.entity.display_name AS entity_display_name
    , o.city_id
    , o.zone_id
    , FORMAT_DATE('%G-%V', CAST(DATETIME(d.rider_dropped_off_at, d.timezone) AS DATE)) AS report_week_local
    , COUNT(DISTINCT d.id) AS count_deliveries -------- exclude preorders ?
    , COUNT(DISTINCT(IF(o.is_preorder IS FALSE, o.order_id, 0))) AS count_orders
    , SUM(IF(o.is_preorder IS FALSE, o.timings.actual_delivery_time / 60, NULL)) AS delivery_time_sum
    , COUNT(IF(o.is_preorder IS FALSE, o.timings.actual_delivery_time / 60, NULL)) AS delivery_time_count
    , SUM(IF(o.is_preorder IS FALSE, o.timings.promised_delivery_time / 60, NULL)) AS expected_delivery_time_sum
    , COUNT(IF(o.is_preorder IS FALSE, o.timings.promised_delivery_time / 60, NULL)) AS expected_delivery_time_count
    , COUNT(DISTINCT(IF(o.timings.order_delay / 60 > 10, o.order_id, NULL))) AS sum_delayed_orders
  FROM `{{ params.project_id }}.cl.orders` o
  LEFT JOIN UNNEST(deliveries) d
  WHERE o.created_date BETWEEN (SELECT start_time FROM parameters) AND (SELECT end_time FROM parameters)
    AND d.delivery_status = 'completed'
  GROUP BY 1, 2, 3, 4--, 5
)
SELECT u.country_code
  , co.country_name
  , IF(ARRAY_LENGTH(co.platforms) = 1, co.platforms[OFFSET(0)].display_name, en.entities) AS entity_display_name
  , co.region
  , u.city_id
  , ci.name AS city_name
  , u.zone_id
  , z.name AS zone_name
  , CASE
      WHEN u.report_week_local = FORMAT_DATE('%G-%V', '{{ next_ds }}')
        THEN 'current_week'
      WHEN u.report_week_local = FORMAT_DATE('%G-%V', DATE_SUB('{{ next_ds }}', INTERVAL 1 WEEK))
        THEN '1_week_ago'
      WHEN u.report_week_local = FORMAT_DATE('%G-%V', DATE_SUB('{{ next_ds }}', INTERVAL 2 WEEK))
        THEN '2_weeks_ago'
      WHEN u.report_week_local = FORMAT_DATE('%G-%V', DATE_SUB('{{ next_ds }}', INTERVAL 3 WEEK))
        THEN '3_weeks_ago'
      WHEN u.report_week_local = FORMAT_DATE('%G-%V', DATE_SUB('{{ next_ds }}', INTERVAL 4 WEEK))
        THEN '4_weeks_ago'
      WHEN u.report_week_local = FORMAT_DATE('%G-%V', DATE_SUB('{{ next_ds }}', INTERVAL 5 WEEK))
        THEN '5_weeks_ago'
      WHEN u.report_week_local = FORMAT_DATE('%G-%V', DATE_SUB('{{ next_ds }}', INTERVAL 6 WEEK))
        THEN '6_weeks_ago'
      WHEN u.report_week_local = FORMAT_DATE('%G-%V', DATE_SUB('{{ next_ds }}', INTERVAL 7 WEEK))
        THEN '7_weeks_ago'
      WHEN u.report_week_local = FORMAT_DATE('%G-%V', DATE_SUB('{{ next_ds }}', INTERVAL 8 WEEK))
        THEN '8_weeks_ago'
      ELSE u.report_week_local
    END AS week_relative
  , u.report_week_local
  , w.utr_working_hours
  , u.count_deliveries
  , u.count_orders
  , u.delivery_time_sum
  , u.delivery_time_count
  , u.expected_delivery_time_sum
  , u.expected_delivery_time_count
  , u.sum_delayed_orders
FROM utr_deliveries u
LEFT JOIN worked_hours w ON u.country_code = w.country_code
  AND u.city_id = w.city_id
  AND u.zone_id = w.zone_id
  AND u.report_week_local = w.report_week_local
LEFT JOIN `{{ params.project_id }}.cl.countries` co ON u.country_code = co.country_code
LEFT JOIN UNNEST(co.cities) ci ON u.city_id = ci.id
LEFT JOIN UNNEST(ci.zones) z ON u.zone_id = z.id
LEFT JOIN entities en ON u.country_code = en.country_code
-- removing darkstore pickers to not impact utr as they do not have orders, only working hours
WHERE u.country_code NOT LIKE '%dp%'
;
