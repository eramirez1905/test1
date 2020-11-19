# accounting

Table of accounting entries with each row representing one accounting entry

Partitioned by field `created_date_utc` with each
partition being a time interval of `DAY`

Clustered by field `rdbms_id`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid is unique and represents an accounting entry |
| id | `INTEGER` |  |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| order_uuid | `STRING` |  |
| order_id | `INTEGER` |  |
| charity_local | `FLOAT` |  |
| charity_plus_vat_local | `FLOAT` |  |
| container_charges_local | `FLOAT` |  |
| container_charges_plus_vat_local | `FLOAT` |  |
| delivery_fee_local | `FLOAT` |  |
| delivery_fee_plus_vat_local | `FLOAT` |  |
| delivery_fee_adjustment_local | `FLOAT` |  |
| delivery_fee_adjustment_plus_vat_local | `FLOAT` |  |
| difference_to_minimum_order_value_local | `FLOAT` |  |
| difference_to_minimum_order_value_plus_vat_local | `FLOAT` |  |
| discount_local | `FLOAT` |  |
| discount_plus_vat_local | `FLOAT` |  |
| gross_total_local | `FLOAT` |  |
| joker_commission_base_local | `FLOAT` |  |
| joker_commission_base_plus_vat_local | `FLOAT` |  |
| joker_fee_local | `FLOAT` |  |
| joker_fee_plus_vat_local | `FLOAT` |  |
| products_local | `FLOAT` |  |
| products_vat_local | `FLOAT` |  |
| products_plus_vat_local | `FLOAT` |  |
| rider_tip_local | `FLOAT` |  |
| rider_tip_plus_vat_local | `FLOAT` |  |
| service_fee_local | `FLOAT` |  |
| service_fee_plus_vat_local | `FLOAT` |  |
| vouchers_local | `FLOAT` |  |
| vouchers_plus_vat_local | `FLOAT` |  |
| voucher_ratio_foodpanda | `FLOAT` |  |
| discount_ratio_foodpanda | `FLOAT` |  |
| created_at_utc | `TIMESTAMP` |  |
| created_date_utc | `DATE` |  |
| updated_at_utc | `TIMESTAMP` |  |
| dwh_last_modified_at_utc | `TIMESTAMP` |  |
| [vat_groups](#vatgroups) | `ARRAY<RECORD>` |  |

## vat_groups

| Name | Type | Description |
| :--- | :--- | :---        |
| amount_local | `FLOAT` |  |
| base_amount_local | `FLOAT` |  |
| percentage | `FLOAT` |  |
