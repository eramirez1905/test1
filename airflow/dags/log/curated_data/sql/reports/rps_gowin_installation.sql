CREATE OR REPLACE TABLE `{{ params.project_id }}.rl.rps_gowin_installation`
PARTITION BY created_date
CLUSTER BY country_code, client_version AS
WITH client_installations AS (
  SELECT ci.created_date
    , ci.created_at
    , ci.region
    , ci.country_code
    , ci.device_id
    , ci.client_version
    , ci.installation_action
    , ci.installation_result
    , insta.process_stage AS installation_process_stage
    , insta.package_file_name
    , insta.time_span
    , ci.os_name
    , ci.windows_version
  FROM `{{ params.project_id }}.cl.rps_vendor_client_installations` ci
  CROSS JOIN UNNEST(installation) insta
  WHERE ci.device_id IS NOT NULL
    -- Get rid of all Verions with the old 1.6.x verions
    AND NOT SPLIT(ci.client_version, '.')[OFFSET(0)] = '1'
    AND NOT SPLIT(ci.client_version, '.')[OFFSET(1)] = '6'
)
SELECT created_date
  , DATE_TRUNC(created_date, MONTH) AS created_month
  , os_name
  , windows_version
  , country_code
  , client_version
  , installation_result
  , installation_action
  , device_id
  , SUM(IF(installation_process_stage = 'TOTAL', 1, 0)) AS installation_ct
  , SUM(IF(installation_process_stage = 'TOTAL' AND installation_result = 'FAILURE', 1, 0)) AS fail_installation_ct
  , SUM(IF(installation_process_stage = 'TOTAL' AND installation_result = 'SUCCESS', 1, 0)) AS success_installation_ct
  , SUM(IF(time_span > 0 AND installation_process_stage = 'INSTALLATION', time_span, 0 )) AS timespan_installation_sum
  , COUNT(IF(time_span > 0 AND installation_process_stage = 'INSTALLATION', time_span, NULL)) AS timespan_installation_ct
  , SUM(IF(time_span > 0 AND installation_process_stage = 'INSTALLATION' AND installation_action = 'INSTALL', time_span, 0)) AS timespan_installation_install_sum
  , COUNT(IF(time_span > 0 AND installation_process_stage = 'INSTALLATION' AND installation_action = 'INSTALL', time_span, NULL)) AS timespan_installation_install_ct
  , SUM(IF(time_span > 0 AND installation_process_stage = 'INSTALLATION' AND installation_action = 'UPDATE', time_span, 0)) AS timespan_installation_update_sum
  , COUNT(IF(time_span > 0 AND installation_process_stage = 'INSTALLATION' AND installation_action = 'UPDATE', time_span, NULL)) AS timespan_installation_update_ct
  , SUM(IF(time_span > 0 AND installation_process_stage = 'DOWNLOADING_PACKAGE', time_span, 0)) AS timespan_package_download_sum
  , COUNT(IF(time_span > 0 AND installation_process_stage = 'DOWNLOADING_PACKAGE', time_span, NULL)) AS timespan_package_download_ct
  , SUM(IF(time_span > 0 AND installation_process_stage = 'INSTALLING_PACKAGE', time_span, 0)) AS timespan_package_install_sum
  , COUNT(IF(time_span > 0 AND installation_process_stage = 'INSTALLING_PACKAGE', time_span, NULL)) AS timespan_package_install_ct
  , SUM(IF(time_span > 0 AND installation_process_stage = 'DOWNLOADING_PACKAGE' AND package_file_name IN ('GO', 'RESTAURANTAPP') , time_span, 0)) AS timespan_package_download_application_sum
  , COUNT(IF(time_span > 0 AND installation_process_stage = 'DOWNLOADING_PACKAGE' AND package_file_name IN ('GO', 'RESTAURANTAPP'), time_span, NULL)) AS timespan_package_download_application_ct
  , SUM(IF(time_span > 0 AND installation_process_stage = 'DOWNLOADING_PACKAGE' AND package_file_name LIKE '%CEFSHARP%', time_span, 0)) AS timespan_package_download_cefsharp_installer_sum
  , COUNT(IF(time_span > 0 AND installation_process_stage = 'DOWNLOADING_PACKAGE' AND package_file_name LIKE '%CEFSHARP%', time_span, NULL)) AS timespan_package_download_cefsharp_installer_ct
  , SUM(IF(time_span > 0 AND installation_process_stage = 'DOWNLOADING_PACKAGE' AND package_file_name IN ('VC_RED ST.X86.EXE', 'VC_REDIST.X86.EXE'), time_span, 0)) AS timespan_package_download_visualcplusplus_redist_sum
  , COUNT(IF(time_span > 0 AND installation_process_stage = 'DOWNLOADING_PACKAGE' AND package_file_name IN ('VC_RED ST.X86.EXE', 'VC_REDIST.X86.EXE'), time_span, NULL)) AS timespan_package_download_visualcplusplus_redist_ct
  , SUM(IF(time_span > 0 AND installation_process_stage = 'DOWNLOADING_PACKAGE' AND package_file_name IN ('NDP452_KB2901954_WEB.EXE', 'NDP461_KB3102438_WEB.EXE'), time_span, 0)) AS timespan_package_download_dotnetframework_sum
  , COUNT(IF(time_span > 0 AND installation_process_stage = 'DOWNLOADING_PACKAGE' AND package_file_name IN ('NDP452_KB2901954_WEB.EXE', 'NDP461_KB3102438_WEB.EXE'), time_span, NULL)) AS timespan_package_download_dotnetframework_ct
  , SUM(IF(time_span > 0 AND installation_process_stage = 'DOWNLOADING_PACKAGE' AND (package_file_name = '' OR (STARTS_WITH(package_file_name, '{') AND ENDS_WITH(package_file_name, '}'))), time_span, 0)) AS timespan_package_download_others_sum
  , COUNT(IF(time_span > 0 AND installation_process_stage = 'DOWNLOADING_PACKAGE' AND (package_file_name = '' OR (STARTS_WITH(package_file_name, '{') AND ENDS_WITH(package_file_name, '}'))), time_span, NULL)) AS timespan_package_download_others_ct
  , SUM(IF(time_span > 0 AND installation_process_stage = 'INSTALLING_PACKAGE' AND package_file_name IN ('GO', 'RESTAURANTAPP'), time_span, 0)) AS timespan_package_install_application_sum
  , COUNT(IF(time_span > 0 AND installation_process_stage = 'INSTALLING_PACKAGE' AND package_file_name IN ('GO', 'RESTAURANTAPP'), time_span, NULL)) AS timespan_package_install_application_ct
  , SUM(IF(time_span > 0 AND installation_process_stage = 'INSTALLING_PACKAGE' AND package_file_name LIKE '%CEFSHARP%', time_span, 0)) AS timespan_package_install_cefsharp_installer_sum
  , COUNT(IF(time_span > 0 AND installation_process_stage = 'INSTALLING_PACKAGE' AND package_file_name LIKE '%CEFSHARP%', time_span, NULL)) AS timespan_package_install_cefsharp_installer_ct
  , SUM(IF(time_span > 0 AND installation_process_stage = 'INSTALLING_PACKAGE' AND package_file_name IN ('VC_RED ST.X86.EXE', 'VC_REDIST.X86.EXE'), time_span, 0)) AS timespan_package_install_visualcplusplus_redist_sum
  , COUNT(IF(time_span > 0 AND installation_process_stage = 'INSTALLING_PACKAGE' AND package_file_name IN ('VC_RED ST.X86.EXE', 'VC_REDIST.X86.EXE'), time_span, NULL)) AS timespan_package_install_visualcplusplus_redist_ct
  , SUM(IF(time_span > 0 AND installation_process_stage = 'INSTALLING_PACKAGE' AND package_file_name IN ('NDP452_KB2901954_WEB.EXE', 'NDP461_KB3102438_WEB.EXE'), time_span, 0)) AS timespan_package_install_dotnetframework_sum
  , COUNT(IF(time_span > 0 AND installation_process_stage = 'INSTALLING_PACKAGE' AND package_file_name IN ('NDP452_KB2901954_WEB.EXE', 'NDP461_KB3102438_WEB.EXE'), time_span, NULL)) AS timespan_package_install_dotnetframework_ct
  , SUM(IF(time_span > 0 AND installation_process_stage = 'INSTALLING_PACKAGE' AND (package_file_name = '' OR (STARTS_WITH(package_file_name, '{') AND ENDS_WITH(package_file_name, '}'))), time_span, 0)) AS timespan_package_install_others_sum
  , COUNT(IF(time_span > 0 AND installation_process_stage = 'INSTALLING_PACKAGE' AND (package_file_name = '' OR (STARTS_WITH(package_file_name, '{') AND ENDS_WITH(package_file_name, '}'))), time_span, NULL)) AS timespan_package_install_others_ct
FROM client_installations
GROUP BY 1,2,3,4,5,6,7,8,9
