# ncr_braze_campaigns

Table of NCR campaigns run in Braze. Result of MKT Braze fetcher filtered by relevant campaigns and joined to NCR data.

| Name                | Type        | Description                                                                                                           |
| :------------------ | :---------- | :-------------------------------------------------------------------------------------------------------------------- |
| braze_external_id   | `STRING`    | external_id used in Braze, e.g. email for Talabat or \<CC>_user_id for Foodpanda. **This column is considered PII.**  |
| midas_booking_id    | `INTEGER`   | booking_id received from MIDAS NCR database                                                                           |
| source_id           | `INTEGER`   | source_id corresponds to global DWH Redshift `dwh_il.dim_countries`                                                   |
| entity_id           | `STRING`    | entity_id corresponds to source_code in global DWH Redshift `dwh_il.dim_countries`                                    |
| customer_id         | `STRING`    | customer_id in Global DWH                                                                                             |
| ncr_package_type    | `STRING`    |                                                                                                                       |
| ncr_vendor_id       | `STRING`    |                                                                                                                       |
| ncr_publish_date    | `DATE`      |                                                                                                                       |
| campaign_name       | `STRING`    | Campaign name in Braze                                                                                                |
| last_received       | `TIMESTAMP` |                                                                                                                       |
| in_control          | `BOOLEAN`   |                                                                                                                       |
| opened_notification | `BOOLEAN`   | `TRUE` if either of `opened_push` OR `clicked_email` OR `opened_email` OR `clicked_in_app_message` is `TRUE` in Braze |
