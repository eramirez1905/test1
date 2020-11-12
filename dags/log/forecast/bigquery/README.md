The `forecasting` schema is home to the input data for the order forecast. 
It consists of aggregated order, event and other data (`*_timeseries`) per half hour per zone (`zones`)
and combined datasets (`dataset_*`) that are finally persisted in our data lake 
for further processing.

## Orders to zones logic

First all orders will be mapped to zones w.r.t. to the following logic:
* dropoff or pickup location is within zone AND
* if order is halal order, zone has to be halal zone AND
* if zone has distance cap `d`, order must have delivery distance below `d`.

If there are multiple matches, the following logic generates ranks:
1. dropoff is in zone,
2. dropoff and pickup is in zone,
3. zone with smallest distance cap (will prefer `Walker` zones if they are a matched)
4. zone with smallest area 
5. zone with smallest id

## Events

The `events` table contains events from Porygon, Rooster (Outages) 
and Pandora platform (legacy events).

## Events to zones logic

Assigns events to zones using geographical overlap.
If the event comes with a geometry, it calculates the ratio of how much the event geometry overlaps the 
zone and vice versa. Unrelated event to zone relations (tiny overlaps) are then discarded.
If the event comes without a geometry but with a city, it is assigned to all zones of the city.

## Timeseries data

All of the `*_timeseries` contain half hourly features per country and zone.
This includes `orders_timeseries`, `events_timeseris` and some other tables. 
The tables can be joined by using `(country_code, zone_id, datetime)`, 
in some cases another attribute is additionally needed, for ex. for `events_timeseries` the columns 
`(country_code, zone_id, datetime, event_type)` are the primary key.
All datetime columns are in UTC, all zone ids are the ones derived 
from geo API (which can be found in `zones`).

## Validation of extracted features

Some queries are validated using the `BigQueryCheckOperator`, the queries are stored in `validation`.

## Feature store & validation of combined features

After the extraction of all features the results are combined in tables `dataset_*` (for ex. `dataset_zone_hour`).
Before the results are split into country-level data and exported to the feature store a validation 
is applied on a country level. The quers are stored in `feature_store/validation`.
