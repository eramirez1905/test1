CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.dispatching_report_v2`
PARTITION BY report_date AS
SELECT lo.country_code
  , lo.user_id
  , al.email
  , DATETIME(al.created_at, al.timezone) AS created_at_local
  , CAST(DATETIME(al.created_at, al.timezone) AS DATE) AS report_date
  , CASE
      WHEN FORMAT_DATE('%G-%V', CAST(DATETIME(al.created_at, al.timezone) AS DATE)) = FORMAT_DATE('%G-%V', '{{ next_ds }}')
        THEN 'current_week'
      ELSE FORMAT_DATE('%G-%V', CAST(DATETIME(al.created_at, al.timezone) AS DATE))
    END AS report_week
  , COUNT(lo.log_id) AS logs
  , COUNTIF (al.action LIKE 'cancel_order') AS cancel_order
  , COUNTIF (al.action LIKE 'advance_delivery_status') AS advance_delivery_status
  , COUNTIF (al.action LIKE 'change_dropoff_address') AS change_dropoff_address
  , COUNTIF (al.action LIKE 'courier_updated') AS courier_updated
  , COUNTIF (al.action LIKE 'create_shift') AS create_shift
  , COUNTIF (al.action LIKE 'delivery_time_updated') AS delivery_time_updated
  , COUNTIF (al.action LIKE 'deactivate_shift') AS deactivate_shift
  , COUNTIF (al.action LIKE 'force_connect') AS force_connect
  , COUNTIF (al.action LIKE 'manual_dispatch' AND al.hurrier.manual_dispatch.status LIKE 'queued') AS manual_undispatch
  , COUNTIF (al.action LIKE 'manual_dispatch' AND al.hurrier.manual_dispatch.status LIKE 'dispatched') AS manual_dispatch
  , COUNTIF (al.action LIKE 'replace_delivery') AS replace_delivery
  , COUNTIF (al.action LIKE 'send_to_vendor') AS send_to_vendor
  , COUNTIF (al.action LIKE 'update_courier_route') AS update_courier_route
  , COUNTIF (al.action LIKE 'update_delivery_status') AS update_delivery_status
  , COUNTIF (al.action LIKE 'update_order') AS update_order
  , COUNTIF (al.action LIKE 'update_shift' OR al.action LIKE 'courier_break' OR al.action LIKE 'finish_ongoing_shift' OR al.action LIKE 'courier_temp_not_working') AS update_shift
  , COUNTIF (al.action LIKE 'courier_working') AS courier_working
  , COUNTIF (al.action LIKE 'courier_break') AS courier_break
FROM (
  SELECT DISTINCT country_code
    , user_id
    , log_id
  FROM `{{ params.project_id }}.cl.audit_logs`
  WHERE created_date >= DATE_SUB('{{ next_ds }}', INTERVAL 6 WEEK)
  -- removing darkstore pickers to not impact utr as they do not have orders, only working hours
    AND country_code NOT LIKE '%dp%'
     ) lo
LEFT JOIN `{{ params.project_id }}.cl.audit_logs` al USING(country_code, user_id, log_id)
GROUP BY 1, 2, 3, 4, 5, 6
