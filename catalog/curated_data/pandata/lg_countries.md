# lg_countries

Table of countries in logistics


| Name | Type | Description |
| :--- | :--- | :---        |
| rdbms_id | `INTEGER` |  |
| region | `STRING` |  |
| region_short_name | `STRING` |  |
| code | `STRING` | Each country code is unique and represents a country |
| iso | `STRING` |  |
| name | `STRING` |  |
| currency_code | `STRING` |  |
| [platforms](#platforms) | `ARRAY<RECORD>` |  |
| [cities](#cities) | `ARRAY<RECORD>` |  |

## platforms

| Name | Type | Description |
| :--- | :--- | :---        |
| display_name | `STRING` |  |
| is_active | `BOOLEAN` |  |
| entity_id | `STRING` |  |
| brand_id | `STRING` |  |
| hurrier_platforms | `ARRAY<STRING>` |  |
| rps_platforms | `ARRAY<STRING>` |  |
| brand_name | `STRING` |  |
| [dps_config](#dpsconfig) | `ARRAY<RECORD>` |  |

## dps_config

| Name | Type | Description |
| :--- | :--- | :---        |
| [travel_time](#traveltime) | `ARRAY<RECORD>` |  |
| [delivery_fee_range](#deliveryfeerange) | `ARRAY<RECORD>` |  |

## travel_time

| Name | Type | Description |
| :--- | :--- | :---        |
| travel_time_formula | `STRING` |  |
| travel_time_multiplier | `FLOAT` |  |
| created_at | `TIMESTAMP` |  |
| updated_at | `TIMESTAMP` |  |

## delivery_fee_range

| Name | Type | Description |
| :--- | :--- | :---        |
| min_delivery_fee | `NUMERIC` |  |
| max_delivery_fee | `NUMERIC` |  |
| created_at | `TIMESTAMP` |  |
| updated_at | `TIMESTAMP` |  |

## cities

| Name | Type | Description |
| :--- | :--- | :---        |
| id | `INTEGER` |  |
| name | `STRING` |  |
| is_active | `BOOLEAN` |  |
| order_value_limit_local | `FLOAT` |  |
| timezone | `STRING` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| [zones](#zones) | `ARRAY<RECORD>` |  |
| [goal_delivery_time_history](#goaldeliverytimehistory) | `ARRAY<RECORD>` |  |

## zones

| Name | Type | Description |
| :--- | :--- | :---        |
| id | `INTEGER` |  |
| name | `STRING` |  |
| geo_id | `STRING` |  |
| fleet_id | `STRING` |  |
| is_active | `BOOLEAN` |  |
| is_embedded | `BOOLEAN` |  |
| has_default_delivery_area_settings | `BOOLEAN` |  |
| delivery_types | `ARRAY<STRING>` |  |
| vehicle_types | `ARRAY<STRING>` |  |
| distance_cap_in_meters | `INTEGER` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| area_in_km_square | `FLOAT` |  |
| boundaries | `GEOGRAPHY` |  |
| embedding_zone_ids | `ARRAY<INTEGER>` |  |
| [shape](#shape) | `RECORD` |  |
| [default_delivery_area_settings](#defaultdeliveryareasettings) | `ARRAY<RECORD>` |  |
| [starting_points](#startingpoints) | `ARRAY<RECORD>` |  |
| [events](#events) | `ARRAY<RECORD>` |  |
| [opening_times](#openingtimes) | `ARRAY<RECORD>` |  |
| [goal_delivery_time_history](#goaldeliverytimehistory) | `ARRAY<RECORD>` |  |

## shape

| Name | Type | Description |
| :--- | :--- | :---        |
| geojson | `STRING` |  |
| wkt | `STRING` |  |
| geo | `GEOGRAPHY` |  |
| updated_at_utc | `TIMESTAMP` |  |

## default_delivery_area_settings

| Name | Type | Description |
| :--- | :--- | :---        |
| [delivery_fee](#deliveryfee) | `RECORD` |  |
| delivery_time_in_minutes | `FLOAT` |  |
| municipality_tax | `FLOAT` |  |
| municipality_tax_type | `STRING` |  |
| tourist_tax | `FLOAT` |  |
| tourist_tax_type | `STRING` |  |
| status | `STRING` |  |
| minimum_value_local | `FLOAT` |  |
| drive_time_in_minutes | `INTEGER` |  |
| vehicle_profile | `STRING` |  |
| is_cut_on_zone_border | `BOOLEAN` |  |
| priority | `INTEGER` |  |
| [filters](#filters) | `ARRAY<RECORD>` |  |

## delivery_fee

| Name | Type | Description |
| :--- | :--- | :---        |
| amount_local | `FLOAT` |  |
| percentage | `FLOAT` |  |

## filters

| Name | Type | Description |
| :--- | :--- | :---        |
| key | `STRING` |  |
| [conditions](#conditions) | `ARRAY<RECORD>` |  |

## conditions

| Name | Type | Description |
| :--- | :--- | :---        |
| operator | `STRING` |  |
| values | `ARRAY<STRING>` |  |

## starting_points

| Name | Type | Description |
| :--- | :--- | :---        |
| id | `INTEGER` |  |
| name | `STRING` |  |
| is_active | `BOOLEAN` |  |
| demand_distribution | `NUMERIC` |  |
| created_at_utc | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| [shape](#shape) | `RECORD` |  |

## shape

| Name | Type | Description |
| :--- | :--- | :---        |
| geojson | `STRING` |  |
| wkt | `STRING` |  |
| geo | `GEOGRAPHY` |  |

## events

| Name | Type | Description |
| :--- | :--- | :---        |
| id | `INTEGER` |  |
| action | `STRING` |  |
| activation_threshold | `INTEGER` |  |
| deactivation_threshold | `INTEGER` |  |
| title | `STRING` |  |
| value | `INTEGER` |  |
| message | `STRING` |  |
| vertical_type | `STRING` |  |
| shape_geo | `GEOGRAPHY` |  |
| is_halal | `BOOLEAN` |  |
| is_shape_in_sync | `BOOLEAN` |  |
| start_time_local | `TIMESTAMP` |  |
| end_time_local | `TIMESTAMP` |  |
| updated_at_utc | `TIMESTAMP` |  |
| delivery_providers | `ARRAY<STRING>` |  |
| cuisines | `ARRAY<STRING>` |  |
| tags | `ARRAY<STRING>` |  |
| characteristics | `ARRAY<STRING>` |  |

## opening_times

| Name | Type | Description |
| :--- | :--- | :---        |
| weekday | `STRING` |  |
| start_time_local | `TIME` |  |
| end_time_local | `TIME` |  |

## goal_delivery_time_history

| Name | Type | Description |
| :--- | :--- | :---        |
| goal_delivery_time_in_minutes | `INTEGER` |  |
| updated_at_utc | `TIMESTAMP` |  |

## goal_delivery_time_history

| Name | Type | Description |
| :--- | :--- | :---        |
| goal_delivery_time_in_minutes | `INTEGER` |  |
| updated_at_utc | `TIMESTAMP` |  |
