# Content

The `forecasting` schema is home to the input data for the order forecast. 
It consists of aggregated order and event data plus different other useful features:

* **zones**: relevant zone data extracted from `dwh.zones`
* **cities**: relevant city data extracted from `dwh.cities` including loading timeframes for incremental loads
* **deliveries**: relevent deliveries metadata extracted from `dwh.deliveries`
* **timeseries**: basic half hour time series grid that should be used to join other time series data
* **orders_to_zones**: one to one mapping from order to zone
* **events_to_zones**: one to many mapping from event to zones
* **orders_timeseries**: orders timeseries derived from `orders_to_zones` mapping, `dwh.orders` and `timeseries`
* **events_timeseries**: events timeseries (by event type) derived from `events_to_zones` mapping, `dwh.events` and `timeseries`
* **opening_hours_timeseries**: opening hours timeseries derived from `legacy_merge_layer.forecast_utr_targets` and `timeseries`

Most of these tables can be joined by using their natural ids or primary keys like (country_code, zone_id, datetime) 
All datetime columns are in UTC, all zone ids are the external zone ids.

# Business logic 

More specific details on the business logic of the assignment of events and orders to zones.

## events

### events_to_zones

Assigns events from `dwh.events` to zones from `dwh.zones`.
If the event comes with a geometry, it calculates the ratio of how much the event geometry overlaps the 
zone and vice versa. Unrelated event to zone relations (tiny overlaps) are then discarded.
If the event comes without a geometry but with a city, it is assigned to all zones of the city.

* Primary key: `country_code, event_id, zone_id`.
* Note: `metadata` contains the relation of the event to the zone.

### events_timeseries

Aggregates events from `dwh.events` on a half hourly zone level using `events_to_zones` by `event_type`.
The data can be used to join with other half hour timeseries like `orders_timeseries`.

* Primary key: `country_code, zone_id, datetime, event_type`
* Note: `metadata` contains an array of related event ids and the `value_` entries describe the values of the event during this time bin.

## orders

### orders_to_zones

Assigns orders from `dwh.orders` to zones from `dwh.zones` using the pickup and dropoff location of the order.
The matching is done in 2 steps:
1. Mapping orders to all fitting zones using the following logic:
    1. order's dropoff and pickup location are within zone polygon,
    2. (if zone has `distance_cap`) `delivery_distance` is below the `distance_cap`, 
    3. if order has tag `halal` and zone `tag_halal` set to `true`, or, if order does not have tag `halal`, zone's `tag_halal` is `false`.

2. Resolve multiple matches in the following way (using `DISTINCT ON`):
    * 0 zones matched: set `zone_id` to `0`
    * 1 zone matched: done.
    * 2 or more zones are matched: 
        1. prefer lowest `distance_cap` if given.
        2. prefer zone where pickup happened.
        3. prefer zone with smaller area.

* Primary key: `country_code, order_id`
* Note: `metadata` contains arrays of all dropoff and pickup zone ids.

### orders_timeseries

Aggregates orders from `dwh.orders` on a half hourly zone level using `orders_to_zones`.
The data can be used to join with other half hour timeseries like `events_timeseries`.

* Primary key: `country_code, zone_id, datetime`
* Note: `tags` and `status` contains aggregations of the according orders.

### Combining the results

See [forecastwrapper](https://github.com/foodora/logistics-pykit/blob/master/forecastwrapper/order_query.sql).
