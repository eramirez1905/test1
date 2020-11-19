# newsletter_users

Table of newsletter users with each row representing one person who signed up for the newsletter

Clustered by field `rdbms_id`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` |  |
| id | `INTEGER` |  |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| customer_id | `INTEGER` |  |
| mailing_id | `STRING` |  |
| area_id | `INTEGER` |  |
| city_id | `INTEGER` |  |
| created_by_user_id | `INTEGER` |  |
| updated_by_user_id | `INTEGER` |  |
| email | `STRING` |  |
| first_name | `STRING` |  |
| last_name | `STRING` |  |
| language | `STRING` |  |
| mailing_name | `STRING` |  |
| is_active | `BOOLEAN` |  |
| subscribed_at_local | `TIMESTAMP` |  |
| unsubscribed_at_local | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |
