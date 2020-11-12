# Warehouses (latest)

Warehouses are where inventory for darkstores are held in. Orders are fulfilled by warehouses. This table consists of information with regards to the warehouses which includes the currency it operates in and the time it has been active.

## Warehouses

| Column             | Type            | Description                                                                               |
| :----------------- | :-------------- | :---------------------------------------------------------------------------------------- |
| entity_name | `STRING` | Entity name |
| country_code | `STRING` | Country |
| warehouse_id | `INTEGER` | Warehouse id from Odoo |
| warehouse_name | `STRING` | Warehouse name from Odoo |
| vendor_code | `STRING` | Vendor code from Odoo |
| currency_code | `STRING` | Currency code of warehouse from Odoo |
| timpstamp_active | `TIMESTAMP` | The timestamp of the first order fulfilled by warehouse |
| age_in_months | `INTEGER` | The number of months the warehouse has been active |
| age_in_weeks | `INTEGER` | The number of weeks the warehouse has been active |
| age_in_days | `INTEGER` | The number of days the warehouse has been active |

### Table sync Frequency

Daily
