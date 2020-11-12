# blacklisted_phone_numbers

Table of phone numbers blacklisted from the platform

Clustered by field `rdbms_id`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid is unique and represents a blacklisted phone number |
| id | `INTEGER` |  |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| phone_number | `STRING` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |
