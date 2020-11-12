-- sendbird_channel_members
CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_gcc_service.sendbird_channel_members`
CLUSTER BY country_code, channel_url, member_id AS
SELECT lower(country_iso) AS country_code
  , * EXCEPT(country_iso)
FROM `{{ params.project_id }}.dl_gcc_service.gcc_sendbird_channel_members`
