# Orders products by vendor (daily aggregates)

This table contains information on minimum and maximum price and discount as well as amount of units sold and revenue
for a particular vendor and particular product. It combines data from `pelican` and `catalog` apps.

## Product Summary Day

| Column             | Type            | Description                                                                               |
| :----------------- | :-------------- | :---------------------------------------------------------------------------------------- |
| created_date       | `DATE`          | Date in ISO 8061 format when an order was placed                                          |
| vendor_global_id   | `STRING`        | Global id of vendor from datafridge                                                       |
| product_global_id  | `STRING`        | Global id of product from datafridge                                                      |
| category_hashes    | `ARRAY<STRING>` | An array containing hashes of categories a particular product belonged to on a given date |
| brand              | `STRING`        | An brand definition a particular product belonged to on a given date                      |
| min_price          | `DECIMAL`       | A minimum price required for a product on a given date                                    |
| max_price          | `DECIMAL`       | A maximum price required for a product on a given date                                    |
| min_discount_price | `DECIMAL`       | A minimum discount price required for a product on a given date                           |
| max_discount_price | `DECIMAL`       | A maximum discount price required for a product on a given date                           |
| campaigns          | `ARRAY<STRING>` | An array of campaign names that were active for a product on a given date                 |
| revenue            | `DECIMAL`       | Revenue obtained by a particular vendor for a product on a given day                      |
| units_sold         | `INTEGER`       | Units sold by a particular vendor for a product on a given day                            |
| country_code       | `STRING`        | Country code                                                                              |
| chain_id           | `STRING`        | Chain id for a particular product                                                         |
| sku                | `STRING`        | Stock keeping unit id                                                                     |

### Price aggregates for dates

The query that builds the table contains a piece of logic that deals with price aggregates for order dates. If the price change event in catalog
has `updated_date` that is earlier than order date (`placed_at`) then the last event from `catalog_vendor_product_price` applies, however,
if the date the order was placed on is the same as the price change event date than a combination of price change events is required.

The aggregates for prices are done by date in ISO 8601 format, sometimes the prices change a few times a day. In order to build min/max
aggregates all price change events from a date an order was placed need to be taken into account as well as the latest event before that date.
In order to achieve that two dependent rankings are applied as follows:

| Order placed at | Price change date | Ranking by date | Ranking by date and time |
| :-------------- | :---------------- | :-------------- | :----------------------- |
| 2020-07-20      | 2020-07-20        | 1               | 1                        |
| 2020-07-20      | 2020-07-20        | 1               | 2                        |
| 2020-07-20      | 2020-07-18        | 2               | 1                        |
| 2020-07-20      | 2020-07-18        | 2               | 2                        |
| 2020-07-20      | 2020-07-01        | 1               | 1                        |

From this table we need to construct min/max prices for order that happened on date 2020-07-20. In that case the date for price changes on
that day needs to be aggregated (rows 1 and 2), moreover, the day started with some price that has been adjusted by preceding event, so the latest
event before the date the order was placed applies, in the case above this is row 3 (latest event from 2020-07-18 which is represented
by a combination of rankings - 2, 1).

### Date naming conventions

In table specification for Insights app the main filter date is `day_start`. Because `day_start` field is the main filtering column it should
be used for partitioning. However, the convention for all tables in curated layer (logistics) is to partition always by a `created_date` column.
This way all curated layer users can expect that each table has the same column for partitioning and they do not have to refer to documentation
each time they start working with new resources. Taking that into consideration column name `created_date` is used, although a row represents
an aggregate per day of mutliple events.

### Table sync Frequency

'0 1,5,9,13,17,21 * * *' => Every day At 01:00 AM, 05:00 AM, 09:00 AM, 01:00 PM, 05:00 PM and 09:00 PM
