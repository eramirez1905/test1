# translations

Table of translation with each row representing one translation

Clustered by field `rdbms_id`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid is unique and represents a translation |
| id | `INTEGER` |  |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| language_id | `INTEGER` |  |
| object_id | `INTEGER` |  |
| object_type | `STRING` |  |
| object_attribute | `STRING` |  |
| object_text | `STRING` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |
