# Dynamic pricing user sessions


| Column | Type | Description |
| :--- | :--- | :--- | :--- |
| region | `STRING` | The region assigned to the country of the vendor. |
| country_code | `STRING` | A two-character alphanumeric code based on the country of the vendor as specified by ISO 3166-1 ALPHA-2. |
| entity_id | `STRING` | The code of the local operational entity representing the country. This code is made out of two codes : the two-letter code of the venture, and the country iso. |
| created_at | `TIMESTAMP` | The timestamp when this row of record is created in UTC. |
| endpoint | `STRING` | The endpoint(singleFee) from where the request is coming. |
| version | `STRING` | The version of data source.  |
| [customer](#customer) | `RECORD` | Customer information. |
| [vendors](#vendors) | `ARRAY<RECORD>` | List of Vendor information. |
| promised_delivery_time | `TIMESTAMP`| The date and time the order is supposed to be delivered to the customer. |
| created_date | `STRING` | The partition column. The date when this row of record was created. |

## Customer

| Column | Type | Description |
| :--- | :--- | :--- |
| id | `STRING`| An identifier for customer. |
| user_id | `STRING`| The ID of the user. |
| [session](#session) | `RECORD` | Session information. |
| location | `GEOGRAPHY`| Location of the customer sent by the platform. |
| apply_abtest | `BOOL` | Flag to identify the AB test where version is V1. |
| variant | `STRING` | String to identify the AB test where version is V2. |
| tag | `STRING` | String to identify the tag where version is V2. |

## Session

| Column | Type | Description |
| :--- | :--- | :--- |
| id | `STRING`| An identifier for the session. |
| timestamp | `TIMESTAMP`| Timestamp of the session created . |

## Vendors

| Column | Type | Description |
| :--- | :--- | :--- |
| id | `STRING`| An identifier for the vendor. |
| [delivery_fee](#delivery-fee) | `RECORD` | Records pertaining to specifications of the delivery_fee. |
| [minimum_order_value](#minimum-order-value) | `RECORD` | Customer information. |
| tags | `ARRAY<STRING>`| List of tags associated with delivery fee. |

## Delivery fee

| Column | Type | Description |
| :--- | :--- | :--- |
| total | `NUMERIC`| The total value of the delivery fee. |
| travel_time | `NUMERIC`| The travel time of the delivery. |
| fleet_utilisation | `NUMERIC`| The fleet utilisation of the delivery fee. |

## Minimum order value

| Column | Type | Description |
| :--- | :--- | :--- |
| total | `NUMERIC`| The minimum order value to place an order for the vendor within the delivery area. |
