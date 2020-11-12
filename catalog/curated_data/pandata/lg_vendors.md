# lg_vendors

Table of vendors in logistics. A vendor in pandora could belong to more than one entity in logistics.

Clustered by field `rdbms_id`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid is unique and represents a vendor. |
| lg_entity_id | `STRING` |  |
| code | `STRING` | Platform code. Same as pandora's. |
| country_code | `STRING` |  |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| pd_vendor_id | `INTEGER` | Pandora's vendor id |
| name | `STRING` |  |
| vehicle_profile | `STRING` |  |
| location_geo | `GEOGRAPHY` |  |
| last_provided_location_geo | `GEOGRAPHY` |  |
| location_history_geos | `ARRAY<GEOGRAPHY>` |  |
| [hurrier](#hurrier) | `ARRAY<RECORD>` |  |
| [rps](#rps) | `ARRAY<RECORD>` |  |
| [delivery_areas](#deliveryareas) | `ARRAY<RECORD>` |  |
| [delivery_area_locations](#deliveryarealocations) | `ARRAY<RECORD>` |  |
| [porygon](#porygon) | `ARRAY<RECORD>` |  |
| [dps](#dps) | `ARRAY<RECORD>` |  |
| [time_buckets](#timebuckets) | `ARRAY<RECORD>` |  |

## hurrier

| Name | Type | Description |
| :--- | :--- | :---        |
| country_code | `STRING` |  |
| id | `INTEGER` |  |
| [city](#city) | `RECORD` |  |
| name | `STRING` |  |
| order_value_limit_local | `FLOAT` |  |
| last_provided_location_geo | `GEOGRAPHY` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |

## city

| Name | Type | Description |
| :--- | :--- | :---        |
| id | `INTEGER` |  |
| name | `STRING` |  |

## rps

| Name | Type | Description |
| :--- | :--- | :---        |
| region | `STRING` |  |
| country_iso | `STRING` |  |
| is_active | `BOOLEAN` |  |
| is_latest | `BOOLEAN` |  |
| operator_code | `STRING` |  |
| rps_vendor_id | `INTEGER` |  |
| vendor_name | `STRING` |  |
| contract_plan | `STRING` |  |
| delivery_type | `STRING` |  |
| delivery_platform | `STRING` |  |
| rps_global_key | `STRING` |  |
| service | `STRING` |  |
| is_monitor_enabled | `BOOLEAN` |  |
| timezone | `STRING` |  |
| updated_at_utc | `TIMESTAMP` |  |
| [client](#client) | `RECORD` |  |
| [pos](#pos) | `RECORD` |  |
| [address](#address) | `RECORD` |  |
| [contracts](#contracts) | `RECORD` |  |
| [is_monitor_enabled_history](#ismonitorenabledhistory) | `ARRAY<RECORD>` |  |

## client

| Name | Type | Description |
| :--- | :--- | :---        |
| name | `STRING` |  |
| pos_integration_flow | `STRING` |  |

## pos

| Name | Type | Description |
| :--- | :--- | :---        |
| integration_code | `STRING` |  |
| integration_name | `STRING` |  |
| is_pos_integration_active | `BOOLEAN` |  |
| chain_name | `STRING` |  |
| chain_code | `STRING` |  |

## address

| Name | Type | Description |
| :--- | :--- | :---        |
| street | `STRING` |  |
| building | `STRING` |  |
| postal_code | `STRING` |  |
| city_name | `STRING` |  |
| location | `GEOGRAPHY` |  |

## contracts

| Name | Type | Description |
| :--- | :--- | :---        |
| id | `INTEGER` |  |
| country_code | `STRING` |  |
| region | `STRING` |  |
| operator_code | `STRING` |  |
| name | `STRING` |  |
| delivery_type | `STRING` |  |
| value | `INTEGER` |  |
| is_deleted | `BOOLEAN` |  |
| is_enabled | `BOOLEAN` |  |

## is_monitor_enabled_history

| Name | Type | Description |
| :--- | :--- | :---        |
| is_monitor_enabled | `BOOLEAN` |  |
| updated_at_utc | `TIMESTAMP` |  |
| is_latest | `BOOLEAN` |  |

## delivery_areas

| Name | Type | Description |
| :--- | :--- | :---        |
| id | `STRING` |  |
| country_code | `STRING` |  |
| platform | `STRING` |  |
| is_deleted | `BOOLEAN` |  |
| [history](#history) | `ARRAY<RECORD>` |  |

## history

| Name | Type | Description |
| :--- | :--- | :---        |
| transaction_id | `INTEGER` |  |
| end_transaction_id | `INTEGER` |  |
| name | `STRING` |  |
| drive_time_in_minutes | `INTEGER` |  |
| operation_type | `STRING` |  |
| shape_geo | `GEOGRAPHY` |  |
| active_from_utc | `TIMESTAMP` |  |
| active_to_utc | `TIMESTAMP` |  |
| [edited_by](#editedby) | `RECORD` |  |
| [settings](#settings) | `RECORD` |  |

## edited_by

| Name | Type | Description |
| :--- | :--- | :---        |
| user_id | `INTEGER` |  |
| email | `STRING` |  |

## settings

| Name | Type | Description |
| :--- | :--- | :---        |
| [delivery_fee](#deliveryfee) | `RECORD` |  |
| expected_delivery_time_in_minutes | `FLOAT` |  |
| municipality_tax | `FLOAT` |  |
| municipality_tax_type | `STRING` |  |
| tourist_tax | `FLOAT` |  |
| tourist_tax_type | `STRING` |  |
| status | `STRING` |  |
| minimum_value_local | `FLOAT` |  |

## delivery_fee

| Name | Type | Description |
| :--- | :--- | :---        |
| amount_local | `FLOAT` |  |
| percentage | `FLOAT` |  |

## delivery_area_locations

| Name | Type | Description |
| :--- | :--- | :---        |
| lg_city_id | `INTEGER` |  |
| country_code | `STRING` |  |
| city_name | `STRING` |  |

## porygon

| Name | Type | Description |
| :--- | :--- | :---        |
| country_code | `STRING` |  |
| platform | `STRING` |  |
| global_entity_id | `STRING` |  |
| name | `STRING` |  |
| location | `GEOGRAPHY` |  |
| is_active | `BOOLEAN` |  |
| is_halal | `BOOLEAN` |  |
| vertical_type | `STRING` |  |
| delivery_providers | `ARRAY<STRING>` |  |
| cuisines | `ARRAY<STRING>` |  |
| tags | `ARRAY<STRING>` |  |
| customer_types | `ARRAY<STRING>` |  |
| characteristics | `ARRAY<STRING>` |  |
| [polygon_drive_times](#polygondrivetimes) | `ARRAY<RECORD>` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |

## polygon_drive_times

| Name | Type | Description |
| :--- | :--- | :---        |
| vehicle_profile | `STRING` |  |
| drive_time | `INTEGER` |  |
| shape | `GEOGRAPHY` |  |
| created_at | `TIMESTAMP` |  |
| updated_at | `TIMESTAMP` |  |

## dps

| Name | Type | Description |
| :--- | :--- | :---        |
| scheme_id | `INTEGER` |  |
| variant | `STRING` |  |
| scheme_name | `STRING` |  |
| currency | `STRING` |  |
| is_active | `BOOLEAN` |  |
| is_key_account | `BOOLEAN` |  |
| is_scheme_fallback | `BOOLEAN` |  |
| [vendor_config](#vendorconfig) | `ARRAY<RECORD>` |  |

## vendor_config

| Name | Type | Description |
| :--- | :--- | :---        |
| id | `INTEGER` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| [pricing_config](#pricingconfig) | `ARRAY<RECORD>` |  |

## pricing_config

| Name | Type | Description |
| :--- | :--- | :---        |
| [travel_time_config](#traveltimeconfig) | `RECORD` |  |
| [mov_config](#movconfig) | `RECORD` |  |
| [delay_config](#delayconfig) | `RECORD` |  |

## travel_time_config

| Name | Type | Description |
| :--- | :--- | :---        |
| id | `INTEGER` |  |
| name | `STRING` |  |
| is_fallback | `BOOLEAN` |  |
| threshold_in_minutes | `NUMERIC` |  |
| fee_local | `NUMERIC` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |

## mov_config

| Name | Type | Description |
| :--- | :--- | :---        |
| id | `INTEGER` |  |
| name | `STRING` |  |
| is_fallback | `BOOLEAN` |  |
| travel_time_threshold_in_minutes | `NUMERIC` |  |
| minimum_order_value_local | `NUMERIC` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |

## delay_config

| Name | Type | Description |
| :--- | :--- | :---        |
| id | `INTEGER` |  |
| name | `STRING` |  |
| is_fallback | `BOOLEAN` |  |
| delay_limit_threshold_in_minutes | `NUMERIC` |  |
| travel_time_threshold_in_minutes | `NUMERIC` |  |
| fee_local | `NUMERIC` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |

## time_buckets

| Name | Type | Description |
| :--- | :--- | :---        |
| id | `STRING` |  |
| created_by_lg_user_id | `STRING` |  |
| updated_by_lg_user_id | `STRING` |  |
| day_of_week | `STRING` |  |
| time_of_day_type | `STRING` |  |
| state | `STRING` |  |
| preparation_time_in_seconds | `INTEGER` |  |
| preparation_buffer_in_seconds | `INTEGER` |  |
| active_from_utc | `TIMESTAMP` |  |
| active_to_utc | `TIMESTAMP` |  |
