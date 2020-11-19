# Dynamic Pricing Config

DPS configuration table for the different pricing schemes setup. 

| Column | Type | Description |
| :--- | :--- | :--- |
| entity_id | `STRING` | Code of the local operational entity in a country. The code is made out of two parts: The two-letter abbreviation of the venture and the country ISO code. |
| vendor_code | `STRING` | Alphanumeric identifier of a vendor that is unique per entity_id. |
| vendor_name | `STRING` | Name shown to (external) customers or used by internal users to recognize the vendor. |
| country_code | `STRING` | A two-character alphanumeric code based on the code of the country. |
| is_active | `BOOLEAN` | A two-character alphanumeric code based on the code of the country. |
| is_key_account | `BOOLEAN` | Boolean for indicating if the vendor is a key account for the platform. |
| delivery_types | `<ARRAY>RECORD` | Type of delivering goods from vendor to customer. |
| vertical_type | `STRING` | Type of business vertical a vendor belongs to. |
| customer_types | `<ARRAY>RECORD` | Type of customers a vendor serves. |
| currency | `STRING` | Monetary unit a vendor accepts. |
| chain_id | `STRING` | Identifier for a chain a vendor belongs to. |
| chain_name | `STRING` | Name of a chain a vendor belongs to. |
| location_geojson | `STRING` | Location of a vendor in GeoJSON format. |
| location_wkt | `STRING` | Location of a vendor in WKT format. |
| created_at | `TIMESTAMP` | Timestamp of when the vendor config was created. | 
| updated_at | `TIMESTAMP` | Timestamp of when the vendor config was updated. | 
| location_wkb | `BYTES` | Location of a vendor in WKB format. |
| price_config_variant | `STRING` | Variant of a price configuration for a/b/n testing. |
| scheme_id | `NUMERIC` | Identifier of the scheme. |
| scheme_name | `STRING` | Name of the scheme. | 
| is_scheme_fallback | `BOOLEAN` | Boolean indicating if the scheme is a fallback or was specifically assigned to vendor. |
| [travel_time_fee_config](#travel-time-fee-config) | `RECORD` | Configuration of delivery fees that are based on travel time. |
| [mov_fee_config](#mov-fee-config) | `RECORD` | Configuration of minimum order values that are based on travel time. |
| [delay_fee_config](#delay-fee-config) | `RECORD` | Configuration of delivery fees that are based on fleet delay. |

## Travel Time Fee Config

| Column | Type | Description |
| :--- | :--- | :--- |
| config_id | `STRING` | Identifier of a travel time fee configuration. |
| is_fallback | `BOOLEAN` | Boolean for indicating if the  travel time configuration is a fallback or was specifically assigned to vendor. |
| travel_time_threshold | `NUMERIC` | Upper limit of travel time for which a delivery fee value applies. |
| fee | `NUMERIC` | Delivery fee value that applies at respective travel time. |
| name | `STRING` | Name of the travel time configuration; used for identification. |
| created_at | `TIMESTAMP` | Creation date and time of travel time configuration. |
| updated_at | `TIMESTAMP` | Update date and time of travel time configuration. |

## MOV Fee Config

| Column | Type | Description |
| :--- | :--- | :--- |
| config_id | `STRING`| Identifier of a minimum order value configuration. |
| is_fallback | `BOOLEAN` | Boolean for indicating if the minimum order value configuration is a fallback or was specifically assigned to vendor. |
| travel_time_threshold |  `NUMERIC` | Upper limit of travel time for which a minimum order value applies. |
| fee |  `NUMERIC` | Minimum order value that applies at respective travel time. |
| name | `STRING` | Name of the minimum order value configuration; used for identification. |
| created_at | `TIMESTAMP` | Creation date and time of minimum order value configuration. |
| updated_at | `TIMESTAMP` | Update date and time of minimum order value configuration. |

## Delay Fee Config

| Column | Type | Description |
| :--- | :--- | :--- |
| config_id | `STRING`| Identifier of a fleet delay configuration. |
| is_fallback | `BOOLEAN` | Boolean for indicating if the delay configuration is a fallback or was specifically assigned to vendor. |
| travel_time_threshold |  `NUMERIC` | Upper limit of travel time for which a delay applies. |
| fee |  `NUMERIC` | Delivery fee discount/surcharge that applies at respective travel time. |
| name | `STRING` | Name of the fleet delay fee configuration; used for identification. |
| created_at | `TIMESTAMP` | Creation date and time of fleet delay fee configuration. |
| updated_at | `TIMESTAMP` | Update date and time of fleet delay fee configuration. |
