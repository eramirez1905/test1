# Marketing Orders

This table contains records about attributed acquisitions globally (countries & entities distinguished via source_id). All online channels get their share from every single acquisition & reorder. As attribution model 40-20-40 distribution is used for every touch point

| Column                                 | Type      | Description                            |
| :------------------------------------- | :-------- | :------------------------------------- |
| entity_code                            | `STRING`  | Entity and country code e.g `FP_PK`    |
| source_id                              | `INTEGER` | entity and country id in dim_countries |
| dataset                                | `INTEGER` |                                        |
| platform_type                          | `STRING`  | Web or App                             |
| platform                               | `STRING`  | Web, Android, IOS, Mobile web          |
| order_date                             | `DATE`    |                                        |
| channel_subchannel                     | `STRING`  |                                        |
| source                                 | `STRING`  |                                        |
| medium                                 | `STRING`  |                                        |
| campaign                               | `STRING`  | Campaign name                          |
| acquisitions                           | `FLOAT`   |                                        |
| acquisitions_calibration               | `FLOAT`   |                                        |
| acquisitions_with_vouchers             | `FLOAT`   |                                        |
| acquisitions_with_vouchers_calibration | `FLOAT`   |                                        |
| reorders                               | `FLOAT`   |                                        |
| reorders_with_voucher                  | `FLOAT`   |                                        |
| orders                                 | `FLOAT`   |                                        |
| orders_with_voucher                    | `FLOAT`   |                                        |
| amt_gmv_eur                            | `FLOAT`   |                                        |
| amt_gmv_usd                            | `FLOAT`   |                                        |
| amt_gmv_lc                             | `FLOAT`   |                                        |
| amt_voucher_eur                        | `FLOAT`   |                                        |
| amt_voucher_usd                        | `FLOAT`   |                                        |
| amt_voucher_lc                         | `FLOAT`   |                                        |
| amt_commission_eur                     | `FLOAT`   |                                        |
| amt_commission_usd                     | `FLOAT`   |                                        |
| amt_commission_lc                      | `FLOAT`   |                                        |
| amt_gmv_eur_acq_tov_7d                 | `FLOAT`   |                                        |
| amt_gmv_eur_acq_tov_14d                | `FLOAT`   |                                        |
| amt_gmv_eur_acq_tov_28d                | `FLOAT`   |                                        |
