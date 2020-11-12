# Campaigns

| Column | Type | Description |
| :--- | :--- | :--- |
| country_code | `STRING`| A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2. |
| id | `STRING`| Identifier of the campaign in Porygon application. |
| [campaigns](#campaigns) | `ARRAY<RECORD>`| Array containing current and historical details of campaigns. |

### campaigns

| Column | Type | Description |
| :--- | :--- | :--- |
| id | `STRING`| Identifier of the campaign in Porygon application. |
| is_deleted | `BOOLEAN`| Flag indicating if the campaign is currently deleted. |
| [history](#history) | `ARRAY<RECORD>`| History of the campaigns. |

### History

| Column | Type | Description |
| :--- | :--- | :--- |
| active_from | `TIMESTAMP`| The datetime from when the attributes of the campaign start to be active. |
| active_to | `TIMESTAMP`| The datetime from when the attributes of the campaign stopped to be active. If the campaign is still active, the value is `NULL`. |
| operation_type | `STRING`| It specifies the action on the campaign. It can be `created`, `updated` or `cancelled`. |
| shape | `GEOGRAPHY`| The geometrical shape of the delivery area. |
| transaction_id | `INTEGER`| The identifier created when campaign was created or updated. |
| end_transaction_id | `INTEGER`| The identifier created when campaign was updated. |
| is_active | `BOOLEAN`| Flag indicating if the campaign is currently active or not. |
| name | `STRING`| The name associated to the campaign if the user who created it choose one. |
| drive_time | `INTEGER`| The drive time associated to the delivery area. |
| [settings](#settings)| `RECORD`| Records pertaining to specifications of the campaign. Note: not all fields exist in every country. |
| [filters](#filters) | `ARRAY<RECORD>`| Extra information coming from the vendor updates provided by the platforms. |

### Settings

| Column | Type | Description |
| :--- | :--- | :--- |
| [delivery fee](#delivery-fee)| `RECORD`| Records pertaining to specifications of the delivery fee. |
| delivery_time | `FLOAT`| The expected delivery time for the delivery area. |
| municipality_tax | `FLOAT`| Value indicating how much is the municipality tax for the delivery area. |
| municipality_tax_type | `STRING`| Type of the municipality tax. |
| status | `STRING`| The status of the delivery area (Eg: open, busy). |
| minimum_value | `FLOAT`| The minimum order value to place an order for the vendor within the delivery area. |

### Delivery Fee

| Column | Type | Description |
| :--- | :--- | :--- |
| amount | `FLOAT`| The value of the delivery fee. |
| percentage | `FLOAT`| The percentage of the delivery fee. |

#### Filters
| Column | Type | Description |
| :--- | :--- | :--- |
| key | `STRING`| The key of the object. For example `delivery_types`, `chains`. |
| [conditions](#conditions) | `ARRAY<RECORD>`| Conditions to which the `key` applies. |

#### Conditions
| Column | Type | Description |
| :--- | :--- | :--- |
| operator | `STRING`| It further specifies what the condition does. Example: `$is` = `is` , `$not` = `is not`, `$nin` = `not in`, `$all` = `all`, `$in` = `in`. |
| values | `STRING`| The value that the `key` might have. For example `platform_delivery`. |
