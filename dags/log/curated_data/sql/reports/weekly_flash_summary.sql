CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.weekly_flash_summary`
PARTITION BY report_date AS
WITH parameters AS (
  SELECT DATE_SUB('{{ next_ds }}', INTERVAL 2 YEAR) AS start_date
), dataset AS (
  SELECT o.country_code
    , o.order_id
    , COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone)) AS report_date
    , FORMAT_DATE("%G-%V", COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone)))
        AS start_dateweek
    , FORMAT_DATE("%Y-%m", COALESCE(DATE(d.rider_dropped_off_at, o.timezone), DATE(d.created_at, o.timezone)))
        AS start_date_month
    , (SELECT COUNT(d.id) FROM UNNEST (deliveries) d WHERE d.delivery_status = 'completed') AS deliveries
    , d.delivery_status
    , o.order_status
    , o.timezone
    , o.entity.display_name AS entity_display_name
    , o.is_preorder
    , d.id AS delivery_id
    , d.rider_id
    , (o.timings.actual_delivery_time / 60) AS actual_delivery_time_mins
  FROM `{{ params.project_id }}.cl.orders` o
  LEFT JOIN UNNEST (deliveries) d ON d.is_primary
  WHERE o.created_date >= (SELECT start_date FROM parameters)
), merge_source_data AS (
  SELECT 'hurrier_orders' AS _source
    , co.region
    , co.country_name
    , d.country_code
    , d.report_date
    , d.start_dateweek
    , d.start_date_month
    , d.entity_display_name
    , COUNT(IF(d.delivery_status = 'completed', d.order_id, NULL)) AS orders_completed
    , COUNT(IF(d.order_status = 'cancelled', d.order_id, NULL)) AS orders_cancelled
    , SUM(IF(is_preorder IS FALSE AND d.delivery_status = 'completed', actual_delivery_time_mins, 0)) AS dt_numerator
    , COUNT(IF(is_preorder IS FALSE AND d.delivery_status = 'completed', d.order_id, NULL)) AS dt_denominator
    , COUNT(IF(actual_delivery_time_mins > 45 AND is_preorder IS FALSE AND d.delivery_status = 'completed',
        actual_delivery_time_mins, NULL)) AS dt_over_45_count
    , COUNT(IF(actual_delivery_time_mins < 20 AND is_preorder IS FALSE AND d.delivery_status = 'completed',
        actual_delivery_time_mins, NULL)) AS dt_below_20_count
    , 0 AS working_hours_rooster
    , 0 AS break_hours_rooster
    , 0 AS costs
    , NULL AS deliveries_payments
    , NULL AS working_hours_payments
  FROM dataset d
  LEFT JOIN `{{ params.project_id }}.cl.countries` co ON d.country_code = co.country_code
  WHERE d.report_date >= (SELECT start_date FROM parameters)
  AND d.country_code NOT LIKE '%dp%'
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

  UNION ALL

  SELECT 'rooster' AS _source
    , co.region
    , co.country_name
    , s.country_code
    , e.day AS report_date
    , FORMAT_DATE("%G-%V", e.day) AS start_dateweek
    , FORMAT_DATE("%Y-%m", e.day) AS start_date_month
    , '' AS entity_display_name
    , 0 AS orders_completed
    , 0 AS orders_cancelled
    , 0 AS dt_numerator
    , 0 AS dt_denominator
    , 0 AS dt_over_45_count
    , 0 AS dt_below_20_count
    , SUM(e.duration / 60 / 60) AS working_hours_rooster
    , SUM(IF(b.duration IS NOT NULL, b.duration / 60 / 60, 0)) AS break_hours_rooster
    , 0 AS costs
    , NULL AS deliveries_payments
    , NULL AS working_hours_payments
  FROM `{{ params.project_id }}.cl.shifts` s
  LEFT JOIN UNNEST(s.actual_working_time_by_date) e
  LEFT JOIN UNNEST(s.actual_break_time_by_date) b ON e.day = b.day
  LEFT JOIN `{{ params.project_id }}.cl.riders` r ON s.country_code = r.country_code
    AND s.rider_id = r.rider_id
  LEFT JOIN `{{ params.project_id }}.cl.countries` co ON s.country_code = co.country_code
  WHERE s.shift_state = 'EVALUATED'
    AND e.status = 'ACCEPTED'
    AND DATE(s.actual_start_at, s.timezone) >= (SELECT start_date FROM parameters)
    AND s.country_code NOT LIKE '%dp%'
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

  UNION ALL

  SELECT 'payment' AS _source
    , p.region
    , p.country_name
    , p.country_code
    , p.report_date_local  AS report_date
    , p.report_week AS start_dateweek
    , p.report_month AS start_date_month
    , NULL AS entity_display_name
    , 0 AS orders_completed
    , 0 AS orders_cancelled
    , 0 AS dt_numerator
    , 0 AS dt_denominator
    , 0 AS dt_over_45_count
    , 0 AS dt_below_20_count
    , 0 AS working_hours_rooster
    , 0 AS break_hours_rooster
    , p.costs_by_country AS costs
    , p.deliveries_payments AS deliveries_payments
    , p.working_hours_payments AS working_hours_payments
  FROM (
    SELECT payment.region
      , payment.country_name
      , payment.country_code
      , payment.report_date_local
      , payment.report_week
      , payment.report_month
      , CAST(SUM(SAFE_DIVIDE(payment.total_date_payment, payment.exchange_rate)) AS FLOAT64) AS costs_by_country
      , SUM(deliveries) AS deliveries_payments
      , SUM (working_hours) AS working_hours_payments
    FROM `{{ params.project_id }}.rl.payment_report` payment
      LEFT JOIN `{{ params.project_id }}.cl.countries` co ON payment.country_code = co.country_code
      LEFT JOIN UNNEST(co.cities) ci ON payment.city_id = ci.id
      LEFT JOIN UNNEST(ci.zones) zo ON payment.zone_id = zo.id
    WHERE payment.report_date_local >= (SELECT start_date FROM parameters)
    AND ci.timezone IS NOT NULL
    AND payment.country_code NOT LIKE '%dp%'
    GROUP BY 1, 2, 3, 4, 5, 6
  ) AS p
  WHERE report_date_local >= (SELECT start_date FROM parameters)
), merge_aggregated_data AS (
  SELECT region
    , report_date
    , country_name
    , country_code
    , start_dateweek
    , start_date_month
    , _source
    , entity_display_name
    , orders_completed
    , orders_cancelled
    , dt_numerator
    , dt_denominator
    , dt_over_45_count
    , dt_below_20_count
    , working_hours_rooster
    , break_hours_rooster
    , costs AS costs_euro
    , deliveries_payments
    , working_hours_payments
  FROM merge_source_data
), merge_aggregated_data_orders AS (
  SELECT o.region
    , o.report_date
    , o.country_name
    , o.country_code
    , o.start_dateweek
    , o.start_date_month
    , o.entity_display_name
    , o.orders_completed
    , o.orders_cancelled
    , o.dt_numerator
    , o.dt_denominator
    , o.dt_over_45_count
    , o.dt_below_20_count
  FROM merge_aggregated_data o
  WHERE _source = 'hurrier_orders'
), merge_aggregated_data_rooster AS (
  SELECT report_date
  , country_code
  , working_hours_rooster
  , break_hours_rooster
  FROM merge_aggregated_data
  WHERE _source = 'rooster'
), merge_aggregated_data_payments AS (
  SELECT report_date
  , country_code
  , deliveries_payments
  , working_hours_payments
  , costs_euro
  FROM merge_aggregated_data
  WHERE _source = 'payment'
)
SELECT o.region
  , o.report_date
  , o.country_name
  , o.country_code
  , o.start_dateweek
  , o.start_date_month
  , o.entity_display_name
  , o.orders_completed
  , o.orders_cancelled
  , o.dt_numerator
  , o.dt_denominator
  , o.dt_over_45_count
  , o.dt_below_20_count
  , r.working_hours_rooster
  , r.break_hours_rooster
  , p.costs_euro
  , p.deliveries_payments
  , p.working_hours_payments
FROM merge_aggregated_data_orders o
LEFT JOIN merge_aggregated_data_rooster r ON o.report_date = r.report_date
  AND o.country_code = r.country_code
LEFT JOIN merge_aggregated_data_payments p ON o.report_date = p.report_date
  AND o.country_code = p.country_code
