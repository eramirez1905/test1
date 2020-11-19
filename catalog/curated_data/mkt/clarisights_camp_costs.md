# clarisights_camp_costs

Table containing marketing channels costs aggregated per campaign and day.
Source of data is Clarisights.

Currently integrated partners: adwords, apple, facebook, jampp, liftoff, moloco,
remerge, smadex, snapchat, tiktok, twitter

| Column        | Type      | Description                                     |
|:--------------|:----------|:------------------------------------------------|
| source_id     | `INTEGER` | DWH source id                                   |
| entity_code   | `STRING`  | Entity and country code e.g `FP_PK`             |
| cost_source   | `STRING`  | Partner name e.g. `adwords`                     |
| campaign      | `STRING`  | Campaign name                                   |
| platform_type | `STRING`  | `web`, `app` or `UNSPECIFIED`                   |
| platform      | `STRING`  | `web`, `android`, `iOS`, `both` or `UNSPECIFED` |
| date          | `DATE`    | Date when costs occured                         |
| cost_eur      | `FLOAT`   | Cost amount in EUR                              |
