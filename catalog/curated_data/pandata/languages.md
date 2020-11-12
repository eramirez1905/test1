# languages

Table of languages with each row representing one language

Clustered by field `rdbms_id`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` |  |
| id | `INTEGER` |  |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| title | `STRING` |  |
| code | `STRING` |  |
| google_map_language | `STRING` |  |
| locale | `STRING` |  |
| is_active | `BOOLEAN` |  |
| is_deleted | `BOOLEAN` |  |
| is_text_direction_right_to_left | `BOOLEAN` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |
| [child_languages](#childlanguages) | `ARRAY<RECORD>` |  |

## child_languages

| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` |  |
| id | `INTEGER` |  |
| title | `STRING` |  |
| code | `STRING` |  |
| google_map_language | `STRING` |  |
| locale | `STRING` |  |
| is_active | `BOOLEAN` |  |
| is_deleted | `BOOLEAN` |  |
| is_text_direction_right_to_left | `BOOLEAN` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |
