# countries

Table of countries with each row representing one country nested with areas and cities

Clustered by field `rdbms_id`


| Name | Type | Description |
| :--- | :--- | :---        |
| id | `INTEGER` | Each id is unique and represents a country |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| lg_rdbms_id | `INTEGER` |  |
| company_id | `INTEGER` |  |
| entity_id | `STRING` |  |
| backend_url | `STRING` |  |
| common_name | `STRING` |  |
| company_name | `STRING` |  |
| iso | `STRING` |  |
| currency_code | `STRING` |  |
| domain | `STRING` |  |
| is_live | `BOOLEAN` |  |
| region | `STRING` |  |
| three_letter_iso_code | `STRING` |  |
| timezone | `STRING` |  |
| vat | `NUMERIC` |  |
| venture_url | `STRING` |  |
| [cities](#cities) | `ARRAY<RECORD>` |  |
| [areas](#areas) | `ARRAY<RECORD>` |  |

## cities

| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` |  |
| id | `INTEGER` |  |
| name | `STRING` |  |
| is_active | `BOOLEAN` |  |
| is_deleted | `BOOLEAN` |  |
| timezone | `STRING` |  |
| vat_rate | `STRING` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |

## areas

| Name | Type | Description |
| :--- | :--- | :---        |
| id | `INTEGER` |  |
| city_id | `INTEGER` |  |
| name | `STRING` |  |
| is_active | `BOOLEAN` |  |
| latitude | `NUMERIC` |  |
| longitude | `NUMERIC` |  |
| postcode | `STRING` |  |
| zoom | `INTEGER` |  |
| [subareas](#subareas) | `ARRAY<RECORD>` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |

## subareas

| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` |  |
| id | `INTEGER` |  |
| name | `STRING` |  |
| is_active | `BOOLEAN` |  |
| latitude | `NUMERIC` |  |
| longitude | `NUMERIC` |  |
| postcode | `STRING` |  |
| zoom | `INTEGER` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |
