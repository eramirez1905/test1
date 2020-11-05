CREATE TEMP FUNCTION clean_os_name(_os_name STRING) AS (
  (SUBSTR(_os_name, STRPOS(_os_name, 'Microsoft')))
);

CREATE TEMP FUNCTION check_os_name(_os_name STRING, _checking_string STRING) AS (
  (SUBSTR(_os_name, STRPOS(_os_name, _checking_string)))
);

CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.rps_vendor_client_installations`
PARTITION BY created_date AS
WITH rps_devices AS (
  SELECT * EXCEPT(_row_number)
  FROM (
    SELECT *
      , ROW_NUMBER() OVER(PARTITION BY region, device_id, vendor_id ORDER BY is_the_latest_version DESC) AS _row_number
    FROM `{{ params.project_id }}.cl.rps_devices`
  )
  WHERE _row_number = 1
), rps_devices_with_vendors AS (
  SELECT *
    , LEAD(assigned_at) OVER (PARTITION BY region, _device_id_for_join, vendor_id ORDER BY assigned_at) AS next_assigned_at
  FROM (
    SELECT region
      , LOWER(country_code) AS country_code
      , UPPER(device_id) AS _device_id_for_join
      , vendor_id
      , assigned_at
      , vendor_code
      , entity_id
    FROM `{{ params.project_id }}.cl.rps_devices`
    )
), gowin_installation_data AS (
  SELECT SAFE_CAST(created_at AS DATE) AS created_date
    , created_at
    , CASE
        WHEN country IN ('AUSTRIA', 'FRANCE', 'GERMANY', 'GREECE', 'ITALY', 'POLAND', 'ROMANIA', 'SPAIN', 'TURKEY', 'UNITED KINGDOM')
          THEN 'eu'
        WHEN country IN ('AUSTRALIA', 'INDIA', "PEOPLE'S REPUBLIC OF CHINA", 'REPUBLIC OF THE PHILIPPINES', 'VIET NAM')
          THEN 'ap'
        WHEN country IN ('ARGENTINA', 'COLOMBIA', 'ECUADOR', 'MEXICO', 'PERU', 'UNITED STATES', 'VENEZUELA')
          THEN 'us'
        WHEN country IN ('BAHRAIN', 'EGYPT', 'JORDAN', 'KUWAIT', 'LEBANON', 'OMAN', 'QATAR', 'SAUDI ARABIA', 'SYRIA', 'U.A.E.')
          THEN 'me'
        WHEN country IN ('KOREA')
          THEN 'kr'
        ELSE NULL
      END AS region
    , CONCAT('WINDOWS|', ins.device_uuid) AS device_id
    , UPPER(CONCAT('WINDOWS|', ins.device_uuid)) AS _device_id_for_join
    , app_version AS client_version
    , CASE
        WHEN STARTS_WITH(check_os_name(os_name, 'Windows 10'),'Windows 10')
          THEN 'Windows 10'
        WHEN STARTS_WITH(check_os_name(os_name, 'Windows 8.1'), 'Windows 8.1')
          THEN 'Windows 8.1'
        WHEN STARTS_WITH(check_os_name(os_name, 'Windows 8'), 'Windows 8')
          THEN 'Windows 8'
        WHEN STARTS_WITH(check_os_name(os_name, 'Windows 7'), 'Windows 7')
          THEN 'Windows 7'
        WHEN clean_os_name(os_name) = 'Microsoft Windows Server 2012 R2 Standard'
          THEN 'Windows Server 2012 R2'
        WHEN clean_os_name(os_name) = 'Microsoft Windows Server 2016 Standard'
          THEN 'Windows Server 2016'
        WHEN clean_os_name(os_name) = 'Microsoft Windows Embedded 8.1 Industry Pro'
          THEN 'Windows Embedded 8.1 Industry'
        WHEN clean_os_name(os_name) = 'Microsoft Windows Embedded Standard'
          THEN 'Windows Embedded Standard 7'
        ELSE '(UNKNOWN)'
      END AS windows_version
    , IF(os_name IS NULL OR os_name = '', '(UNKNOWN)', REPLACE(TRIM(os_name), '?', '')) AS os_name
    , os_version
    , IF(SAFE_CAST(country AS INT64) IS NULL OR country = '', country, '(OTHERS)' )AS country
    , IF(installation_action = ' NSTALL', 'INSTALL', installation_action) AS installation_action
    , installation_result
    , installation_process_stage
    , installation_process_stage_depth
    , IF(package_file_name = '', NULL, package_file_name) AS package_file_name
    , time_span
    , 'GOWIN' AS client_name
  FROM `{{ params.project_id }}.dl.rps_go_win_installation` ins
  -- Partition filter	is set to be `Required` for `rps-gowin-prd.installer.time_span_event`
  WHERE created_date >= '2019-01-01'
), gowin_installation_data_aggreated AS (
  SELECT created_date
    , created_at
    , region
    , country
    , device_id
    , _device_id_for_join
    , client_name
    , client_version
    , CASE
        WHEN TRIM(REPLACE(os_name, '??Microsoft Windows 10 Home', 'Microsoft Windows 10 Home') ) LIKE '%Windows 10%'
          THEN 'Windows 10'
        WHEN TRIM(REPLACE(os_name, '??Microsoft Windows 10 Home', 'Microsoft Windows 10 Home') ) LIKE '%Windows 8.1%'
          THEN 'Windows 8.1'
        WHEN TRIM(REPLACE(os_name, '??Microsoft Windows 10 Home', 'Microsoft Windows 10 Home') ) LIKE '%Windows 8%'
          THEN 'Windows 8'
        WHEN TRIM(REPLACE(os_name, '??Microsoft Windows 10 Home', 'Microsoft Windows 10 Home') ) LIKE '%Windows 7%'
          THEN 'Windows 7'
        WHEN TRIM(REPLACE(os_name, '??Microsoft Windows 10 Home', 'Microsoft Windows 10 Home') ) = 'Microsoft Windows Server 2012 R2 Standard'
          THEN 'Windows Server 2012 R2'
        WHEN TRIM(REPLACE(os_name, '??Microsoft Windows 10 Home', 'Microsoft Windows 10 Home') ) = 'Microsoft Windows Server 2016 Standard'
          THEN 'Windows Server 2016'
        WHEN TRIM(REPLACE(os_name, '??Microsoft Windows 10 Home', 'Microsoft Windows 10 Home') ) = 'Microsoft Windows Embedded 8.1 Industry Pro'
          THEN 'Windows Embedded 8.1 Industry'
        WHEN TRIM(REPLACE(os_name, '??Microsoft Windows 10 Home', 'Microsoft Windows 10 Home') ) = 'Microsoft Windows Embedded Standard'
          THEN 'Windows Embedded Standard 7'
        ELSE NULL
      END AS windows_version
    , os_name
    , os_version
    , installation_action
    , installation_result
    , ARRAY_AGG(
        STRUCT(
          installation_process_stage AS process_stage
          , installation_process_stage_depth AS process_stage_depth
          , package_file_name
          , time_span
        )
      ) AS installation
  FROM gowin_installation_data
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
), gowin_installation_data_with_vendor_id AS (
  SELECT *
    , ROW_NUMBER() OVER (PARTITION BY device_id, created_at ORDER BY assigned_at DESC) AS _row_number
  FROM (
    SELECT COALESCE(d.region, ins.region) AS region
      , d.country_code
      , ins.created_date
      , ins.created_at
      , ins.device_id
      , d.vendor_id
      , d.vendor_code
      , d.entity_id
      , ins.client_name
      , ins.client_version
      , ins.windows_version
      , ins.os_name
      , ins.os_version
      , ins.installation_action
      , ins.installation_result
      , ins.installation
      , d.assigned_at
    FROM gowin_installation_data_aggreated ins
    LEFT JOIN rps_devices_with_vendors d ON ins._device_id_for_join = d._device_id_for_join
      AND ins.created_at BETWEEN d.assigned_at AND COALESCE(d.next_assigned_at, CURRENT_TIMESTAMP)
    )
)
SELECT * EXCEPT (_row_number)
FROM gowin_installation_data_with_vendor_id
WHERE _row_number = 1
