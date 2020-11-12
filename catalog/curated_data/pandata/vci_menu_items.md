# vci_menu_items

Table of vendor menu items from vendor competitive intelligence

Partitioned by field `created_date_utc` with each
partition being a time interval of `DAY`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid is unique and represents a menu item of a vendor in a platform |
| id | `STRING` |  |
| vci_vendor_uuid | `STRING` |  |
| vci_vendor_id | `STRING` |  |
| country_code | `STRING` |  |
| source | `STRING` |  |
| item_name | `STRING` |  |
| price_local | `STRING` |  |
| discounted_price_local | `STRING` |  |
| is_alcohol | `BOOLEAN` |  |
| is_popular | `BOOLEAN` |  |
| description | `STRING` |  |
| image_url | `STRING` |  |
| [category](#category) | `RECORD` |  |
| created_at_utc | `TIMESTAMP` |  |
| created_date_utc | `DATE` | The time when the menu item was scraped in UTC timezone. |
| _ingested_at_utc | `TIMESTAMP` |  |

## category

| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` |  |
| id | `STRING` |  |
| name | `STRING` |  |
| description | `STRING` |  |
| created_at_utc | `TIMESTAMP` |  |
