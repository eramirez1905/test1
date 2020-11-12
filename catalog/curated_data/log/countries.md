# Countries

The `country_code` field is present in **every** dataset we publish. It is 1-1 mapped to the Hurrier instance. For example, Taiwan can be found at https://tw.usehurrier.com and all the entities will have `tw` as the country code.

Most frequently, numerical IDs are unique within a country only, not globally. A composite key of `(country_code, id)` is needed.


| Column | Type | Description |
| :--- | :--- | :--- |
| region | `STRING`| The operational region in which the country is located. The operational regions are: Americas, Asia, Australia, Europe, MENA and Others. |
| region_short_name | `STRING`| The short name of region and Korea is treated as a separate region (kr) to differentiate from others. |
| country_code | `STRING`| The country code used within the logistics tech infrastructure. |
| country_iso | `STRING`| The code of the country as specified by ISO 3166-1 ALPHA-2. |
| country_name | `STRING`| The name of the country in English. |
| currency_code | `STRING`| The code of the currency used in the country as specified by ISO 4217. |
| [platforms](#platforms) | `ARRAY<RECORD>`| N/A |
| [cities](#cities) | `ARRAY<RECORD>`| The collection of cities in the country. |

#### Platforms

| Column | Type | Description |
| :--- | :--- | :--- |
| entity_id | `STRING`| The code of the local operational entity representing the country. This code is made out of two codes : the two-letter code of the venture, and the country iso. |
| brand_id | `STRING`| The code of the global brand operating in the country, identical to the two-letter venture code. |
| hurrier_platforms | `STRING`| The name of the platform (e.g. talabat) on which the order was placed. |
| display_name | `STRING`| N/A |
| is_active | `BOOLEAN`| Whether logistics business is being made in this country using the central logistics services. |
| rps_platforms | `STRING`| The id of the platform on RPS. | 
| brand_name | `STRING`| The name of the brand. | 
| [dps_config](#dps_config) | `ARRAY<RECORD>`| The collection of dps configurations for the entity. |

#### DPS Config

| Column | Type | Description |
| :--- | :--- | :--- |
| [travel_time](#travel-time) | `ARRAY<RECORD>`| The collection of dps travel time configurations for the entity. |
| [delivery_fee_range](#delivery-fee-range) | `ARRAY<RECORD>`| The collection of dps delivery fee ranges for the entity. |

#### Travel Time

| Column | Type | Description |
| :--- | :--- | :--- |
| travel_time_formula | `STRING` | Multiplier per entity used to calculate the travel time. Contains the x intercept. | 
| travel_time_multiplier | `NUMERIC` | Multiplier per entity used to calculate the travel time. The input should be the straight line distance between the vendor and location in kms and the output will be the travel time in minutes.| 
| created_at | `TIMESTAMP` | When the record has been created in UTC. |
| updated_at | `TIMESTAMP` | When the record has been updated in UTC. |

#### Delivery Fee Range

| Column | Type | Description |
| :--- | :--- | :--- |
| min_delivery_fee | `NUMERIC` | Minimum delivery fee range assigned per entity. |
| max_delivery_fee | `NUMERIC` | Maximum delivery fee range assigned per entity. |
| created_at | `TIMESTAMP` | When the record has been created in UTC. |
| updated_at | `TIMESTAMP` | When the record has been updated in UTC. |

#### Cities

Many settings in Hurrier and Rooster can be defined on a city-level. A city further contains multiple zones.

The `id` is unique only within a `country_code`.

| Column | Type | Description |
| :--- | :--- | :--- |
| id | `INTEGER`| The numeric identifier assigned to every city. This id is generated within the central logistics systems and is only unique within the same country. |
| name | `STRING`| Name of the city in English. |
| is_active | `BOOLEAN`| Whether logistics business is being made in this city using the central logistics services. |
| timezone | `STRING`| The name of the timezone where the city is located. The timezone enables time conversion, from UTC to local time and vice versa. |
| order_value_limit | `FLOAT`| Represents the maximum order value in cents until which orders will get dispatched to riders automatically. Orders exceeding this maximum need to be reviewed by dispatchers |
| created_at | `TIMESTAMP`| When the city has been created within the central logistics infrastructure in UTC |
| updated_at | `TIMESTAMP`| When the city has been updated within the central logistics infrastructure in UTC |
| [zones](#zones) | `ARRAY<RECORD>`| The collection of zones in a city. |
| [goal_delivery_time_history](#goal-delivery-time-history) | `ARRAY<RECORD>`| Records pertaining to target delivery time. |

#### Zones

Zones are the primary unit for forecasting orders, riders are then staffed to the zone's starting points.

Hurrier delay is calculated on a zone level.

The `id` is unique only within a `country_code`.


| Column | Type | Description |
| :--- | :--- | :--- |
| id | `INTEGER`| The numeric identifier assigned to the zone. This `id` is generated within the central logistics systems and is only unique within the same country. |
| city_id | `INTEGER` | The numeric identifier of a the city. |
| name | `STRING`| The name of the zone. |
| geo_id | `STRING`| N/A. |
| fleet_id | `STRING`| The fleet id (name) operating in the city. |
| has_default_delivery_area_settings | `BOOLEAN`| Flag indicating if zone has default delivery area settings set up. |
| [default_delivery_area_settings](#default-delivery-area-settings) | `ARRAY<RECORD>`| Set of delivery areas a vendor gets assigned when it is created or when its location changes. The vendor receives the default_deliveryareas_settings of the zone he is located in. More information on the [product portal](https://product.deliveryhero.net/global-logistics/documentation/add-default-delivery-area-for-new-vendor/). |
| shape | `GEOMETRY`| The geometric reference to the polygon of the zone. |
| [zone_shape](#zone-shape) | `RECORD`| **Will be deprecated on December 1st, 2019. Use `shape` instead**<br>The geometric reference to the polygon of the zone. |
| shape_updated_at | `TIMESTAMP`| The date and time when the shape of the zone was last updated within central logistics systems. |
| is_active | `BOOLEAN`| Whether business with the central logistics services is being made in the zone. |
| delivery_types | `ARRAY<STRING>` | List of delivery types. Possible options: <ul><li><code>halal</code></li></ul> |
| vehicle_types | `ARRAY<STRING>` | List of vehicle types. Possible options: <ul><li><code>walk</code></li></ul> |
| distance_cap | `INTEGER`| Only orders with a delivery distance lower than X can be assigned to a zone with a distance cap of X meters. |
| area | `FLOAT`| Area of the polygon in square km. |
| boundaries | `GEOGRAPHY`| Boundaries of the polygon. |
| embedding_zone_ids | `ARRAY<INTEGER>`| Active zones that embed this zone (i.e. this zone lies within the zones `embedding_zone_ids`). |
| is_embedded | `BOOLEAN`| Whether the zone is embedded in another zone. |
| created_at | `TIMESTAMP`| When the zone has been created within the central logistics systems in UTC |
| updated_at | `TIMESTAMP`| When the zone has been updated within the central logistics systems in UTC |
| is_shape_in_sync | `BOOLEAN`| Flag whether an event polygon is kept in sync with the polygon of a zone as referenced by zone_id. |
| [starting_points](#starting-points) | `ARRAY<RECORD>`| The collection of starting points in a zone. |
| [events](#events) | `ARRAY<RECORD>`| Events can reference a zone for sake of automated (de)activation. DAS compares the actual delay of a zone and compares it with the activation and deactivation thresholds of events that reference the zone. It activates the event when the delay exceeds the activation threshold and deactivates the event if the delay falls below the deactivation threshold. More information on the [product portal](https://product.deliveryhero.net/global-logistics/documentation/how-does-automation-of-events-work/). |
| [opening_times](#opening-times) | `ARRAY<RECORD>`| The collection of opening times of the zone. |
| [goal_delivery_time_history](#goal-delivery-time-history) | `ARRAY<RECORD>`| Records pertaining to target delivery time. |

#### Default Delivery Area Settings

| Column | Type | Description |
| :--- | :--- | :--- |
| delivery_fee | `STRING` | A parameters used by PedidosYa. They can be deprecated once pandora moved (as last platform) to a DAS integration via Data Fridge. |
| delivery_time | `FLOAT`| The expected delivery_time for the delivery area. |
| municipality_tax | `FLOAT`| Value indicating how much is the municipality tax for the delivery area. |
| municipality_tax_type | `STRING`| Type of the municipality tax. |
| tourist_tax | `FLOAT`| Tourist tax applied to the delivery area. |
| tourist_tax_type | `STRING`| Type of tourist tax. |
| status | `STRING` | The status of the delivery_area (Eg: open, busy). |
| minimum_value | `FLOAT` | The minimum order value to place an order for the vendor within the delivery area. |
| drive_time | `INTEGER` | The expected drive_time value for the delivery area. |
| vehicle_profile | `STRING` | Vehicle defined by default delivery area. |
| cut_on_zone_border | `BOOLEAN` | It indicates if all existing delivery areas of the selected restaurants are cut by the borders of the zone(s) the restaurant is located in. |
| priority | `INTEGER` | If restaurant is in more than one zone, the default delivery area settings get assigned according to the priority. Higher number corresponds to higher priority. |
| [filters](#filters) | `ARRAY<RECORD>`| Extra information coming from the vendor updates provided by the platforms. |

#### Filters
| Column | Type | Description |
| :--- | :--- | :--- |
| key | `STRING`| The key of the object. For example `delivery_types`, `chains`. |
| [conditions](#conditions) | `ARRAY<RECORD>`| Conditions to which the `key` applies. |

#### Conditions
| Column | Type | Description |
| :--- | :--- | :--- |
| operator | `STRING`| It further specifies what the condition does. Example: `$is` = `is` , `$not` = `is not`, `$nin` = `not in`, `$all` = `all`, `$in` = `in`. |
| values | `STRING`| The value that the `key` might have. For example `platform_delivery`.  |
#### Zone Shape
###### Will be deprecated on December 1st, 2019

| Column | Type | Description |
| :--- | :--- | :--- |
| geojson | `STRING`| The geometric reference to the polygon of the zone in GeoJSON format. |
| wkt | `STRING`| The geometric reference to the polygon of the zone in WKT format. |

#### Goal Delivery Time History

| Column | Type | Description |
| :--- | :--- | :--- |
| goal_delivery_time | `INTEGER`| Shows the target delivery time for specific city or zone. |
| updated_at | `TIMESTAMP`| When the record has been created within the central logistics systems in UTC. |

#### Starting Points

Starting points are the most granular division of geographical areas.
- shifts are planned on a starting point level
- orders are assigned to multiple starting points based on their dropoff address 

The `id` is unique only within a `country_code`.

| Column | Type | Description |
| :--- | :--- | :--- |
| id | `INTEGER`| The numeric identifier assigned to the starting point. This id is generated within the logistics systems and is only unique within the same country. |
| name | `STRING`| The name of the starting point. |
| shape | `GEOMETRY`| The geometric reference to the polygon of the starting point. |
| [starting_point_shape](#starting-point-shape) | `RECORD`| **Will be deprecated on December 1st, 2019. Use `shape` instead**<br>The geometric reference to the polygon of the starting point. |
| is_active | `BOOLEAN`| Whether the starting point is still in use |
| demand_distribution | `FLOAT`| The percentage of staffed riders in the starting point in respect to its zone. Example: if 200 riders are staffed in zone 4 and there are 2 starting points with demand distribution 0.25 and 0.75, then 50 (25% of 200) and 150 (75% of 200) of riders should be staffed accordingly, and the sum of the demand distribution of all starting points in zone 4 should be one so the absolute demand adds up to 200 again. |
| created_at | `TIMESTAMP`| When the record has been created within the central logistics systems in UTC |
| updated_at | `TIMESTAMP`| When the record has been updated within the central logistics systems in UTC |

#### Events

| Column | Type | Description |
| :--- | :--- | :--- |
| id | `INTEGER`| Identifier of the event in Porygon application. |
| action | `STRING`| The type of the event : closure, shrinkage, delay, lockdown. |
| activation_threshold | `INTEGER` | The delay threshold above which an event is activated. |
| deactivation_threshold | `INTEGER` | The delay threshold under which an event is deactivated. |
| starts_at | `TIMESTAMP` | When the event starts in case it is a scheduled event. |
| title | `STRING`| The title of the event. |
| value | `INTEGER`| In the case of shrinkages, the drive time defining the area of the polygon in which the restaurant delivers. |
| message | `STRING`| Event message in all languages. JSON format. |
| updated_at | `TIMESTAMP` | Last updated timestamp of the event. |
| ends_at | `TIMESTAMP` |  When the event ends in case it is a scheduled event. |
| shape | `GEOGRAPHY` | The default delivery area assigned to a vendor. |
| delivery_provider | `STRING`| The type of delivery a vendor is using. Use vendor_delivery when vendor delivers the order themselves. Use pickup when customer picks the order up. Use platform_delivery when platform itself has a rider fleet and delivers the order. Use partner_delivery when neither restaurant or platform deliver the order. |
| cuisines | `ARRAY<STRING>`| The secondary cuisine in which the vendor specialised. |
| is_halal | `BOOLEAN`| Whether the food prepared by the vendor is halal or not. |
| tags | `ARRAY<STRING>` | Vendor tags, for example [ "fast", "asian", "halal" ]. Used for search filtering. Are similar to keywords or Labels in JIRA. |
| vertical_type | `STRING`| Describe the vertical type of a vendor. In other words, it describes types of goods that are delivered. |
| characteristics | `ARRAY<STRING>` | Characteristic of a vendor. |

#### Opening Times

| Column | Type | Description |
| :--- | :--- | :--- |
| weekday | `STRING`| The name of the day of the week when the opening times apply. |
| starts_at | `TIME`| The time from when a zone is open (This time is local). |
| ends_at | `TIME`| The time until when a zone is open (This time is local). |

#### Starting Point Shape
###### Will be deprecated on December 1st, 2019

| Column | Type | Description |
| :--- | :--- | :--- |
| geojson | `STRING`| The geometric reference to the polygon of the starting point in GeoJSON format |
| wkt | `STRING`| The geometric reference to the polygon of the starting point in WKT format |

### Examples:

#### Query cities
```sql
SELECT country_code, c.* EXCEPT(zones)
FROM `fulfillment-dwh-production.curated_data_shared.countries`
LEFT JOIN UNNEST(cities) AS c
ORDER BY country_code, c.id
```

#### Query zones
```sql
SELECT country_code,
  c.id AS city_id,
  c.name AS city_name,
  z.* EXCEPT(starting_points)
FROM `fulfillment-dwh-production.curated_data_shared.countries`
LEFT JOIN UNNEST(cities) AS c
LEFT JOIN UNNEST(c.zones) AS z
ORDER BY country_code, c.id
```

#### Query starting points
```sql
SELECT country_code,
  c.id AS city_id,
  c.name AS city_name,
  z.id AS zone_id,
  z.name AS zone_name,
  sp.*
FROM `fulfillment-dwh-production.curated_data_shared.countries`
LEFT JOIN UNNEST(cities) AS c
LEFT JOIN UNNEST(c.zones) AS z
LEFT JOIN UNNEST(z.starting_points) AS sp
ORDER BY country_code, c.id, z.id
```
