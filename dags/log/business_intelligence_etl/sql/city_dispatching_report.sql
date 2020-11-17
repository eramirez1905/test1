CREATE OR REPLACE TABLE il.city_dispatching_report
PARTITION BY report_date AS
WITH parameters AS (
    SELECT DATE_SUB('{{ next_ds }}', INTERVAL 6 WEEK) AS start_time
), dates AS (
  SELECT date
  FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 40 DAY), DAY), TIMESTAMP_TRUNC(CURRENT_TIMESTAMP, HOUR), INTERVAL 30 MINUTE)) AS date
), report_dates AS (
  SELECT CAST(date AS DATE) AS report_date
    , date AS start_datetime
    , TIMESTAMP_ADD(date, INTERVAL 30 MINUTE) AS end_datetime
  FROM dates
), geog AS (
  SELECT co.country_code
    , co.country_name
    , ci.id as city_id
    , ci.name as city_name
    , zo.id as zone_id
    , zo.name as zone_name
   FROM `{{ params.project_id }}.cl.countries` co
   LEFT JOIN UNNEST(co.cities) ci
   LEFT JOIN UNNEST(ci.zones) zo
), sequence AS ( 
  SELECT country_code
    , city_id
    , zone_id
    , v.vehicle AS vehicle_type
    , d.report_date AS report_date_local
    , CAST(d.start_datetime AS TIME) AS time_local 
  FROM geog
  CROSS JOIN (SELECT * 
              FROM UNNEST(["unknown", "bike", "car"]) AS vehicle) v
  CROSS JOIN report_dates d
), shifts_vehicle AS (
  SELECT DISTINCT s.country_code
    , s.city_id
    , s.zone_id
    , CAST(DATETIME(d.start_datetime, s.timezone) AS DATE) AS report_date_local
    , CAST(DATETIME(d.start_datetime, s.timezone) AS TIME) AS time_local
    , s.courier_id
    , COALESCE(del.vehicle_type, 'unknown') AS vehicle_type
    , ROW_NUMBER() OVER(PARTITION BY s.country_code, s.city_id, s.zone_id, d.start_datetime, s.courier_id) AS first_vehicle
    , CONCAT(CAST(shift_id AS STRING), CAST(repetition_number AS STRING)) AS shift_done
  FROM report_dates d
  LEFT JOIN il.shifts s ON d.report_date = s.shift_start_date
    AND d.start_datetime <= s.shift_start_time
    AND d.end_datetime > s.shift_start_time
  LEFT JOIN (
    SELECT country_code
      , rider_id
      , rider_accepted
      , vehicle.profile AS vehicle_type
    FROM il.deliveries 
    WHERE delivery_status = 'completed'
      AND created_date >= (SELECT start_time FROM parameters)
  ) del ON s.country_code = del.country_code
    AND s.courier_id = del.rider_id
    AND del.rider_accepted BETWEEN s.evaluation_start_time AND s.evaluation_end_time
  WHERE s.created_date >= (SELECT start_time FROM parameters)
), shifts AS (
SELECT country_code
  , city_id
  , zone_id
  , report_date_local
  , time_local
  , courier_id
  , vehicle_type
  , COUNT(DISTINCT shift_done) AS shifts_done
FROM shifts_vehicle
WHERE first_vehicle = 1
GROUP BY 1, 2, 3, 4, 5, 6, 7
), shifts_data AS (
  SELECT country_code
    , city_id
    , zone_id
    , report_date_local
    , time_local
    , vehicle_type
    , SUM(shifts_done) AS shifts_done
  FROM shifts
  GROUP BY 1, 2, 3, 4, 5, 6
), issues_to_zones AS (
  SELECT DISTINCT i.country_code
    , COALESCE(i.city_id, ci.city_id) AS city_id
    , z.zone_id
    , CAST(DATETIME(d.start_datetime, i.timezone) AS DATE) AS report_date_local
    , CAST(DATETIME(d.start_datetime, i.timezone) AS TIME) AS time_local
    , i.issue_id
    , i.order_id
    , del.vehicle.profile AS vehicle_type
    , issue_created_at
    , issue_dismissed_at
    , issue_resolved_at
    , issue_updated_at
    , issue_type
    , issue_category
    , issue_notes
    , CAST(REGEXP_EXTRACT(issue_notes, r'[0-9]+') AS INT64) AS issue_notes_formatted
    , CASE
        WHEN issue_type IN ('CourierIssue')
          AND issue_category IN ('app_issue', 'break_request', 'restaurant_issue', 'equipment_issue', 'locate_customer_issue')
          THEN CONCAT('Reported issues: ', issue_type)
        WHEN issue_type IN ('PickupIssue')
          AND issue_category IN ('waiting')
          THEN 'Waiting at Pickup'
        WHEN issue_type IN ('DispatchIssue')
          AND issue_category IN ('courier_decline')
          THEN 'courier_decline'
        WHEN issue_type IN ('DispatchIssue')
          AND issue_category IN ('no_courier_interaction')
          THEN 'Not accepted'
        WHEN issue_type IN ('DispatchIssue')
          AND issue_category IN ('non_dispatchable_order')
          AND issue_notes IN ('Cannot be dispatched: no couriers in required zones')
          THEN CONCAT ('Cannot be dispatched: ', issue_notes)
        WHEN issue_type IN ('DispatchIssue')
          AND issue_category IN ('non_dispatchable_order')
          AND issue_notes IN ('Cannot be dispatched: no couriers with required vehicles')
          THEN CONCAT ('Cannot be dispatched: ', issue_notes)
        WHEN issue_type IN ('DispatchIssue')
          AND issue_category IN ('non_dispatchable_order')
          AND issue_notes IN ('Cannot be dispatched: none of couriers can deliver it within the current shift')
          THEN CONCAT ('Cannot be dispatched: ', issue_notes)
        WHEN issue_type IN ('DispatchIssue')
          AND issue_category IN ('non_dispatchable_order')
          AND issue_notes IN ('Cannot be dispatched: none of courieres has a vehicle with required capacity')
          THEN CONCAT ('Cannot be dispatched: ', issue_notes)
        WHEN issue_type IN ('DispatchIssue')
          AND issue_category IN ('non_dispatchable_order')
          AND issue_notes IN ('Cannot be dispatched: none of required couriers are working OR no working couriers except forbidden ones')
          THEN CONCAT ('Cannot be dispatched: ', issue_notes)
        WHEN issue_type IN ('DispatchIssue')
          AND issue_category IN ('order_clicked_through')
          THEN 'order_clicked_through'
        WHEN issue_type IN ('DropoffIssue')
          AND issue_category IN ('waiting')
          THEN 'Waiting at Dropoff'
      END AS issue_name
  FROM report_dates d
  LEFT JOIN il.issues i ON d.report_date = i.created_date
    AND d.start_datetime <= i.issue_created_at
    AND d.end_datetime > i.issue_created_at
  LEFT JOIN il.deliveries del ON i.country_code = del.country_code
    AND i.delivery_id = del.delivery_id
    AND del.created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 6 WEEK)
  LEFT JOIN il.cities ci ON i.country_code = ci.country_code
    AND i.city_id = ci.city_id
  LEFT JOIN cl._orders_to_zones AS z ON i.country_code = z.country_code
    AND i.order_id = z.order_id
  WHERE i.created_date >= (SELECT start_time FROM parameters)
), issues AS (
  SELECT i.country_code
    , city_id
    , zone_id
    , i.report_date_local
    , i.time_local
    , COALESCE(i.vehicle_type, 'unknown') AS vehicle_type
    , AVG(IF(issue_notes LIKE '%Waiting at Pickup%', issue_notes_formatted, NULL)) AS Avg_Waiting_at_pickup
    , AVG(IF(issue_notes LIKE '%Waiting at Dropoff%', issue_notes_formatted, NULL)) AS Avg_Waiting_at_dropoff
    , AVG(IF(issue_notes LIKE '%"Accept"%', issue_notes_formatted, NULL)) AS Avg_Courier_did_not_accept
    , SUM(IF(issue_notes IN ('Cannot be dispatched: no courier can be assigned - distance is too long'), TIMESTAMP_DIFF(issue_resolved_at, issue_created_at, SECOND)/60, NULL)) AS limit_for_activity_distances_exceeded_solving_time_sum
    , COUNT(DISTINCT i.issue_id) AS count_issues
    , COUNT(DISTINCT(IF(issue_name = 'Waiting at Pickup', order_id, NULL))) AS o_waiting_at_pickup
    , COUNT(DISTINCT(IF(issue_name = 'Waiting at Pickup', issue_id, NULL))) AS i_waiting_at_pickup
    , COUNT(DISTINCT(IF(issue_name = 'Waiting at Dropoff', order_id, NULL))) AS o_waiting_at_dropoff
    , COUNT(DISTINCT(IF(issue_name = 'Waiting at Dropoff',issue_id, NULL))) AS i_waiting_at_dropoff
    , COUNT(DISTINCT(IF(issue_name = 'order_clicked_through', order_id, NULL))) AS o_order_clicked_through
    , COUNT(DISTINCT(IF(issue_name = 'order_clicked_through', issue_id, NULL))) AS i_order_clicked_through
    , COUNT(DISTINCT(IF(issue_name = 'Not accepted', order_id, NULL))) AS o_rider_not_accepted
    , COUNT(DISTINCT(IF(issue_name = 'Not accepted', issue_id, NULL))) AS i_rider_not_accepted
    , COUNT(DISTINCT(IF(issue_name = 'courier_decline', issue_id, NULL))) AS i_courier_decline
    , COUNT(DISTINCT(IF(issue_type IN ('CourierIssue'), order_id, NULL))) AS o_courier_issue
    , COUNT(DISTINCT(IF(issue_type IN ('CourierIssue'), issue_id, NULL))) AS i_courier_issue
    , COUNT(DISTINCT(IF(issue_notes IN ('Cannot be dispatched: no courier can be assigned - distance is too long'), order_id, NULL))) AS o_limit_for_activity_distances_exceeded_count
    , COUNT(DISTINCT(IF(issue_notes IN ('Cannot be dispatched: no courier can be assigned - distance is too long'), issue_id, NULL))) AS limit_for_activity_distances_exceeded_count
    , COUNT(DISTINCT(IF(issue_category IN ('non_dispatchable_order'), order_id, NULL))) AS o_ds_service_issue
    , COUNT(DISTINCT(IF(issue_category IN ('non_dispatchable_order'), issue_id, NULL))) AS i_ds_service_issue
    , COUNT(DISTINCT(IF(issue_notes LIKE '%"Accept"%' AND (issue_notes_formatted - 2) BETWEEN 0.1 AND 4, issue_id, NULL))) AS Courier_did_not_accept_below_5_mins
    , COUNT(DISTINCT(IF(issue_notes LIKE '%"Accept"%' AND (issue_notes_formatted - 2) BETWEEN 5 AND 9, issue_id, NULL))) AS Courier_did_not_accept_above_5_mins
    , COUNT(DISTINCT(IF(issue_notes LIKE '%"Accept"%' AND (issue_notes_formatted - 2) > 10, issue_id, NULL))) AS Courier_did_not_accept_above_10_mins
    , COUNT(DISTINCT(IF(issue_notes LIKE '%Waiting at Dropoff%' AND (issue_notes_formatted - 6) BETWEEN 0.1 AND 9, issue_id, NULL))) AS Waiting_at_Dropoff_below_10
    , COUNT(DISTINCT(IF(issue_notes LIKE '%Waiting at Dropoff%' AND (issue_notes_formatted - 6) BETWEEN 10 AND 14, issue_id, NULL))) AS Waiting_at_Dropoff_above_10
    , COUNT(DISTINCT(IF(issue_notes LIKE '%Waiting at Dropoff%' AND (issue_notes_formatted - 6) > 15, issue_id, NULL))) AS Waiting_at_Dropoff_above_15
    , COUNT(DISTINCT(IF(issue_notes LIKE '%Waiting at Pickup%' AND (issue_notes_formatted - 15) BETWEEN 0.1 AND 9, issue_id, NULL))) AS Waiting_at_Pickup_below_10
    , COUNT(DISTINCT(IF(issue_notes LIKE '%Waiting at Pickup%' AND (issue_notes_formatted - 15) BETWEEN 10 AND 14, issue_id, NULL))) AS Waiting_at_Pickup_above_10
    , COUNT(DISTINCT(IF(issue_notes LIKE '%Waiting at Pickup%' AND (issue_notes_formatted - 15) BETWEEN 15 AND 180, issue_id, NULL))) AS Waiting_at_Pickup_above_15
  FROM issues_to_zones i
  GROUP BY 1 , 2, 3, 4, 5, 6
), vehicle_to_order AS (
  SELECT DISTINCT country_code
    , order_code
    , order_id
    , FIRST_VALUE(vehicle_type) OVER(PARTITION BY country_code, order_code ORDER BY rider_accepted) AS vehicle_type
  FROM (
  SELECT o.country_code
    , o.order_code
    , o.order_id
    , d.delivery_id
    , d.rider_accepted
    , d.vehicle.profile AS vehicle_type
  FROM il.orders o
  LEFT JOIN il.deliveries d ON o.country_code = d.country_code
    AND o.order_id = d.order_id
    AND d.created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 6 WEEK)
  WHERE delivery_status = 'completed'
    AND o.created_date >= (SELECT start_time FROM parameters)
  )
), audit_step_1 AS (
  SELECT l.country_code
    , COALESCE(l.city_id, ci.city_id) AS city_id
    , l.log_id
    , COALESCE(o.vehicle_type, 'unknown') AS vehicle_type
    , l.action
    , l.old_courier_id
    , l.new_courier_id
    , l.new_courier_email
    , l.old_courier_email
    , l.application
    , l.created_at
    , l.created_date
    , l.timezone
  FROM il.audit_log l
  LEFT JOIN vehicle_to_order o ON l.country_code = o.country_code
    AND l.order_code = o.order_code
  LEFT JOIN il.cities ci ON l.country_code = ci.country_code
    AND l.city_id = ci.city_id
  WHERE l.created_date >= (SELECT start_time FROM parameters)
    AND l.application = 'dashboard'
    AND l.action IN ('courier_updated', 'create_shift', 'deactivate_shift', 'force_connect', 'update_courier_route', 'update_shift', 'courier_break', 'finish_ongoing_shift', 'courier_temp_not_working')
), audit_step_2 AS (
  SELECT DISTINCT l.country_code
    , l.city_id
    , l.log_id
    , l.vehicle_type
    , l.action
    , l.application
    , r.user_id AS courier_id
    , l.created_at
    , l.created_date
    , l.timezone
  FROM audit_step_1 l
  LEFT JOIN `{{ params.project_id }}.ml.hurrier_couriers` r ON l.country_code = r.country_code
    AND (COALESCE(l.new_courier_id, l.old_courier_id) =  r.id
    OR COALESCE(l.new_courier_email, l.old_courier_email) =  r.email)
  WHERE l.application = 'dashboard'
    AND l.action IN ('courier_updated', 'create_shift', 'deactivate_shift', 'force_connect', 'update_courier_route', 'update_shift', 'courier_break', 'finish_ongoing_shift', 'courier_temp_not_working')
), audit_step_with_zones AS (
  SELECT DISTINCT l.country_code
    , l.city_id
    , s.zone_id
    , l.created_at
    , l.created_date
    , l.log_id
    , l.vehicle_type
    , l.action
    , l.timezone
  FROM audit_step_2 l
  LEFT JOIN il.shifts s ON l.country_code = s.country_code
    AND l.created_date = s.shift_start_date
    AND l.courier_id = s.courier_id
), audit AS (
  SELECT l.country_code
    , l.city_id
    , l.zone_id
    , CAST(DATETIME(d.start_datetime, l.timezone) AS DATE) AS report_date_local
    , CAST(DATETIME(d.start_datetime, l.timezone) AS TIME) AS time_local
    , l.vehicle_type
    , COUNTIF(l.action = 'courier_updated') AS courier_updated
    , COUNTIF(l.action = 'create_shift') AS create_shift
    , COUNTIF(l.action = 'deactivate_shift') AS deactivate_shift
    , COUNTIF(l.action = 'force_connect') AS force_connect
    , COUNTIF(l.action = 'update_courier_route') AS update_courier_route
    , COUNTIF(l.action = 'update_shift' OR l.action = 'courier_break' OR l.action = 'finish_ongoing_shift' OR l.action = 'courier_temp_not_working') AS update_shift
  FROM report_dates d
  LEFT JOIN audit_step_with_zones l ON d.report_date = l.created_date
    AND d.start_datetime <= l.created_at
    AND d.end_datetime > l.created_at
  GROUP BY 1, 2, 3, 4, 5, 6
), orders_to_zones_1 AS (
  SELECT DISTINCT o.country_code
    , o.city_id
    , z.zone_id
    , o.order_code
    , o.order_status
    , COALESCE(v.vehicle_type, 'unknown') AS vehicle_type
    , l.log_id
    , l.action
    , o.created_at
    , o.created_date
    , o.timezone
  FROM il.orders o
  LEFT JOIN vehicle_to_order v ON o.country_code = v.country_code
    AND v.order_id = o.order_id
  LEFT JOIN il.audit_log l ON l.country_code = o.country_code
    AND o.order_code = l.order_code
    AND l.created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 6 WEEK)
  LEFT JOIN cl._orders_to_zones z ON o.country_code = z.country_code
    AND o.order_id = z.order_id
  WHERE o.created_date >= (SELECT start_time FROM parameters)
), orders_to_zones_2 AS (
  SELECT DISTINCT o.country_code
    , o.city_id
    , z.zone_id
    , o.order_code
    , o.order_status
    , COALESCE(v.vehicle_type, 'unknown') AS vehicle_type
    , l.action
    , o.created_at
    , o.created_date
    , o.timezone
  FROM il.orders o
  LEFT JOIN vehicle_to_order v ON o.country_code = v.country_code
    AND v.order_id = o.order_id
  LEFT JOIN il.audit_log l ON l.country_code = o.country_code
    AND o.order_code = l.order_code
    AND l.created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 6 WEEK)
  LEFT JOIN cl._orders_to_zones z ON o.country_code = z.country_code
    AND o.order_id = z.order_id
  WHERE o.created_date >= (SELECT start_time FROM parameters)
), temp_order_report_prep_1 AS (
  SELECT o.country_code
    , o.city_id
    , o.zone_id
    , CAST(DATETIME(d.start_datetime, o.timezone) AS DATE) AS report_date_local
    , CAST(DATETIME(d.start_datetime, o.timezone) AS TIME) AS time_local
    , vehicle_type
    , COUNT(DISTINCT o.order_code) AS all_orders
    , COUNT(DISTINCT(IF(o.order_status = 'completed', order_code, NULL))) AS completed_orders
    , COUNT(DISTINCT o.log_id) AS logs
  FROM report_dates d
  LEFT JOIN orders_to_zones_1 o ON d.report_date = o.created_date
    AND d.start_datetime <= o.created_at
    AND d.end_datetime > o.created_at
  WHERE o.country_code IS NOT NULL
  GROUP BY 1, 2, 3, 4, 5, 6
), temp_order_report_prep_2 AS (
  SELECT o.country_code
    , o.city_id
    , o.zone_id
    , CAST(DATETIME(d.start_datetime, o.timezone) AS DATE) AS report_date_local
    , CAST(DATETIME(d.start_datetime, o.timezone) AS TIME) AS time_local
    , vehicle_type
    , COUNTIF(action = 'cancel_order') AS cancel_order
    , COUNTIF(action = 'change_dropoff_address') AS change_dropoff_address
    , COUNTIF(action = 'delivery_time_updated') AS delivery_time_updated
    , COUNTIF(action = 'manual_undispatch') AS manual_undispatch
    , COUNTIF(action = 'manual_dispatch') AS manual_dispatch
    , COUNTIF(action = 'replace_delivery') AS replace_delivery
    , COUNTIF(action = 'send_to_vendor') AS send_to_vendor
    , COUNTIF(action = 'update_courier_route') AS update_courier_route
    , COUNTIF(action = 'update_delivery_status') AS update_delivery_status
    , COUNTIF(action = 'update_order') AS update_order
  FROM report_dates d
  LEFT JOIN orders_to_zones_2 o ON d.report_date = o.created_date
    AND d.start_datetime <= o.created_at
    AND d.end_datetime > o.created_at
  WHERE o.country_code IS NOT NULL
  GROUP BY 1, 2, 3, 4, 5, 6
), temp_order_report_prep_3 AS (
  SELECT o.country_code
    , o.city_id
    , o.zone_id
    , CAST(DATETIME(d.start_datetime, o.timezone) AS DATE) AS report_date_local
    , CAST(DATETIME(d.start_datetime, o.timezone) AS TIME) AS time_local
    , vehicle_type
    , COUNT(DISTINCT order_code) AS orders_touched
  FROM report_dates d
  LEFT JOIN orders_to_zones_2 o ON d.report_date = o.created_date
    AND d.start_datetime <= o.created_at
    AND d.end_datetime > o.created_at
  WHERE action IN ('cancel_order'
                  , 'change_dropoff_address'
                  , 'delivery_time_updated'
                  , 'manual_dispatch'
                  , 'replace_delivery'
                  , 'send_to_vendor'
                  , 'update_courier_route'
                  , 'update_delivery_status'
                  , 'update_order'
                  , 'manual_undispatch')
  GROUP BY 1, 2, 3, 4, 5, 6
), temp_order_report AS (
  SELECT o.country_code
    , o.city_id
    , o.zone_id
    , o.report_date_local
    , o.vehicle_type
    , o.time_local
    , o.all_orders
    , o.completed_orders
    , o.logs
    , oy.orders_touched
    , oz.cancel_order
    , oz.change_dropoff_address
    , oz.delivery_time_updated
    , oz.manual_undispatch
    , oz.manual_dispatch
    , oz.replace_delivery
    , oz.send_to_vendor
    , oz.update_courier_route
    , oz.update_delivery_status
    , oz.update_order
  FROM temp_order_report_prep_1 o
  LEFT JOIN temp_order_report_prep_2 oz USING(country_code, city_id, zone_id, report_date_local, time_local, vehicle_type)
  LEFT JOIN temp_order_report_prep_3 oy USING(country_code, city_id, zone_id, report_date_local, time_local, vehicle_type)
)
SELECT se.country_code
  , se.city_id
  , se.zone_id
  , FORMAT_DATE('%G-%V', se.report_date_local) AS report_week_local
  , se.report_date_local AS report_date
  , se.time_local
  , CAST(CONCAT(CAST(o.report_date_local AS STRING), ' ', CAST(o.time_local AS STRING)) AS DATETIME) AS time_filter
  , CASE
      WHEN FORMAT_DATE('%G-%V', o.report_date_local) = FORMAT_DATE('%G-%V', '{{ next_ds }}')
        THEN 'current_week'
      WHEN FORMAT_DATE('%G-%V', o.report_date_local) = FORMAT_DATE('%G-%V', DATE_SUB('{{ next_ds }}', INTERVAL 1 WEEK))
        THEN '1_week_ago'
      WHEN FORMAT_DATE('%G-%V', o.report_date_local) = FORMAT_DATE('%G-%V', DATE_SUB('{{ next_ds }}', INTERVAL 2 WEEK))
        THEN '2_weeks_ago'
      ELSE FORMAT_DATE('%G-%V', o.report_date_local)
    END AS week_relative
  , g.country_name
  , con.venture_name
  , con.security_group
  , g.city_name
  , g.zone_name
  , se.vehicle_type
  , o.all_orders AS orders
  , o.completed_orders
  , o.orders_touched
  , o.logs
  , o.cancel_order
  , o.change_dropoff_address
  , o.delivery_time_updated
  , o.manual_undispatch
  , o.manual_dispatch
  , o.replace_delivery
  , o.send_to_vendor
  , o.update_courier_route
  , o.update_delivery_status
  , o.update_order
  , a.courier_updated
  , a.create_shift
  , a.deactivate_shift
  , a.force_connect
  , a.update_shift
  , i.i_waiting_at_pickup
  , i.o_waiting_at_pickup
  , i.i_waiting_at_dropoff
  , i.o_waiting_at_dropoff
  , i.i_order_clicked_through
  , i.o_order_clicked_through
  , i.i_rider_not_accepted
  , i.o_rider_not_accepted
  , i.i_courier_decline
  , i.i_courier_issue
  , i.o_courier_issue
  , i.i_ds_service_issue
  , i.o_ds_service_issue
  , i.limit_for_activity_distances_exceeded_count
  , i.o_limit_for_activity_distances_exceeded_count
  , i.limit_for_activity_distances_exceeded_solving_time_sum
  , CASE
      WHEN i.Avg_Waiting_at_pickup >= 14.9
        THEN ROUND(Avg_Waiting_at_pickup, 1)
      ELSE NULL
    END AS avg_waiting_at_pickup
  , CASE
      WHEN i.Avg_Waiting_at_dropoff >= 6
        THEN ROUND(Avg_Waiting_at_dropoff, 1)
      ELSE NULL
    END AS avg_waiting_at_dropoff
  , CASE
      WHEN i.Avg_Courier_did_not_accept >= 2
        THEN ROUND(Avg_Courier_did_not_accept, 1)
      ELSE NULL
    END AS avg_courier_did_not_accept
  , i.courier_did_not_accept_below_5_mins
  , i.courier_did_not_accept_above_5_mins
  , i.courier_did_not_accept_above_10_mins
  , i.waiting_at_dropoff_below_10
  , i.waiting_at_dropoff_above_10
  , i.waiting_at_dropoff_above_15
  , i.waiting_at_pickup_below_10
  , i.waiting_at_pickup_above_10
  , i.waiting_at_pickup_above_15
  , s.shifts_done AS shifts_done
FROM sequence se
LEFT JOIN temp_order_report o USING(country_code, city_id, zone_id, report_date_local, time_local, vehicle_type)
LEFT JOIN audit a USING(country_code, city_id, zone_id, report_date_local, time_local, vehicle_type)
LEFT JOIN issues i USING(country_code, city_id, zone_id, report_date_local, time_local, vehicle_type)
LEFT JOIN shifts_data s USING(country_code, city_id, zone_id, report_date_local, time_local, vehicle_type)
LEFT JOIN geog g ON se.country_code = g.country_code
  AND g.zone_id = se.zone_id
LEFT JOIN il.countries con ON con.country_code = se.country_code
;
