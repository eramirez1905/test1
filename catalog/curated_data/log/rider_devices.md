# Rider Devices


This table shows the device used by the rider during  `screen_opened` events on roadrunner.

| Column | Type | Description |
| :--- | :--- | :--- |
| country_code | `STRING`| A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2. |
| created_date | `DATE`| The date when the event is generated in the application. |
| timezone | `STRING`| The name of the timezone where the country is located. The timezone enables time conversion, from UTC to local time. |
| rider_id | `INTEGER`| Identifier of the rider used by Rooster |
| received_at | `TIMESTAMP`| The timestamp indicating when the event is generated in the application. |
| [device](#device) | `<RECORD>`| Additional information about the device the rider was using. |

### Device

| Column | Type | Description |
| :--- | :--- | :--- |
| category | `STRING`| The type of device (Mobile, Tablet, Desktop). |
| mobile_brand_name | `STRING`| The brand or manufacturer of the device. |
| mobile_model_name | `STRING`| The mobile device model. |
| mobile_marketing_name | `STRING`| The marketing name used for the mobile device. |
| operating_system | `STRING`| The operating system of the device (e.g., "Macintosh" or "Windows"). |
| advertising_id | `STRING`| The unique identifier for the device. The value could be NULL. (e.g. the rider disabled the tracking of the device from Firebase) |
| language | `STRING`| Language settings of the device. |
