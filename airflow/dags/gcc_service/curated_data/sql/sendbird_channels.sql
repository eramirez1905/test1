-- sendbird_channels
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_gcc_service.sendbird_channels`
CLUSTER BY country_code, channel_url AS
SELECT lower(country_iso) AS country_code
  , * EXCEPT(country_iso)
FROM `{{ params.project_id }}.dl_gcc_service.gcc_sendbird_channels`
