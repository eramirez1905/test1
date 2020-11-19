# RPS Connectivity

This dataset comprises of data from Monitor - a service within RPS that tracks the state of RPS restaurants and reacts in non-compliant situations.
This dataset focuses on monitoring devices connectivity (reachable/unreachable) and the actions taken (availability) i.e autoclosing.

Schedules from this dataset are taken from the Data Fridge vendor schedule stream, while connectivity and availability comes directly from Monitor in `dl.rps_monitor`.
 
| Column | Type | Description | Categories |
| :--- | :--- | :--- | :--- |
| region | `STRING` | The region assigned to the country of the vendor. | as, eu, kr, mena and us |
| entity_id | `STRING` | The code of the local operational entity representing the country. This code is made out of two codes : the two-letter code of the venture, and the country iso. | AP_PA, AR_HU, BK_DE, BK_SG, BO_CA, etc |
| country_iso | `STRING` | A two-character alphanumeric code based on the country of the vendor as specified by ISO 3166-1 ALPHA-2. | AE, AR, AT, AU, BA, etc |
| vendor_code | `STRING` | The identifier of the vendor that is sent from the platform (e.g. Foodpanda) to the RPS systems. It is specific to the vendor within a platform. | |
| vendor_id | `INTEGER` | The identifier of a physical vendor within RPS. Only unique within the same `region`. To get a unique id of a record, concatenation of `region` and `vendor_id` is suggested. | |
| timezone | `STRING` | The name of the timezone where the city of the vendor is located. The timezone enables time conversion, from UTC to local time. | |
| is_monitor_enabled | `BOOLEAN` | A flag indicating if the vendor is enabled for unreachable autoclose by Vendor Monitor. | |
| [slots_utc](#slots_utc) | `ARRAY<RECORD>` | Information related to the vendor slots in UTC time. | |
| [slots_local](#slots_local) | `ARRAY<RECORD>` | Information related to the vendor slots in LOCAL time. | |

## Slots UTC
| Column | Type | Description | Categories |
| :--- | :--- | :--- | :--- |
| starts_at | `TIMESTAMP` | When the slot starts in UTC timezone. A slot is a period of time that the vendor is available for receiving orders.| |
| ends_at | `TIMESTAMP` | When the slot ends in UTC timezone. A slot is a period of time that the vendor is available for receiving orders.| |
| duration | `INTEGER` | The time interval between the `slot_start_at` and `slot_end_at` in seconds. | |
| daily_quantity | `INTEGER` | The sum of `slots` within the same date of `slot_start_at`. | |
| daily_schedule_duration | `INTEGER` | The sum of `slot_duration` within the same date of `slot_start_at`. | |
| [connectivity](#connectivity_utc) | `RECORD` | Information related to whether the vendor order reception device is connected / or not to the order transmission network. | |
| [availability](#availability_utc) | `RECORD` | Information related to offline action taken by Vendor Monitor. | |

## Connectivity UTC
| Column | Type | Description | Categories |
| :--- | :--- | :--- | :--- |
| is_unreachable | `BOOLEAN` | A flag indicating if the vendor is unreachable. | |
| unreachable_start_at | `TIMESTAMP` | Timestamp in UTC when the vendor started to be unreachable. | |
| unreachable_end_at | `TIMESTAMP` | Timestamp in UTC when the vendor stopped being unreachable. | |
| unreachable_duration | `INTEGER` | The time interval between the `unreachable_start_at` and `unreachable_end_at` in seconds. | |

## Availability UTC
| Column | Type | Description | Categories |
| :--- | :--- | :--- | :--- |
| is_offline | `BOOLEAN` | A flag indicating if the vendor is taken offline by Vendor Monitor. | |
| offline_reason | `STRING` | The reason why the vendor is taken offline by Vendor Monitor. | UNREACHABLE_OPEN |
| offline_start_at | `TIMESTAMP` | Timestamp in UTC when the vendor is taken offline by Vendor Monitor. | |
| offline_end_at | `TIMESTAMP` | Timestamp in UTC when the vendor become reachable and available to receive orders again. | |
| offline_duration | `INTEGER` | The time interval between the `offline_start_at` and `offline_end_at` in seconds. | |

## Slots Local
| Column | Type | Description | Categories |
| :--- | :--- | :--- | :--- |
| starts_at | `TIMESTAMP` | When the slot starts in Local timezone. A slot is a period of time that the vendor is available for receiving orders.| |
| ends_at | `TIMESTAMP` | When the slot ends in Local timezone. A slot is a period of time that the vendor is available for receiving orders.| |
| duration | `INTEGER` | The time interval between the `slot_start_at` and `slot_end_at` in seconds. | |
| daily_quantity | `INTEGER` | The sum of `slots` within the same date of `slot_start_at`. | |
| daily_schedule_duration | `INTEGER` | The sum of `slot_duration` within the same date of `slot_start_at`. | |
| [connectivity](#connectivity_local) | `RECORD` | Information related to whether the vendor order reception device is connected / or not to the order transmission network. | |
| [availability](#availability_local) | `RECORD` | Information related to offline action taken by Vendor Monitor. | |

## Connectivity Local
| Column | Type | Description | Categories |
| :--- | :--- | :--- | :--- |
| is_unreachable | `BOOLEAN` | A flag indicating if the vendor is unreachable. | |
| unreachable_start_at | `TIMESTAMP` | Timestamp in Local when the vendor started to be unreachable. | |
| unreachable_end_at | `TIMESTAMP` | Timestamp in Local when the vendor stopped being unreachable. | |
| unreachable_duration | `INTEGER` | The time interval between the `unreachable_start_at` and `unreachable_end_at` in seconds. | |

## Availability Local
| Column | Type | Description | Categories |
| :--- | :--- | :--- | :--- |
| is_offline | `BOOLEAN` | A flag indicating if the vendor is taken offline by Vendor Monitor. | |
| offline_reason | `STRING` | The reason why the vendor is taken offline by Vendor Monitor. | UNREACHABLE_OPEN |
| offline_start_at | `TIMESTAMP` | Timestamp in Local when the vendor is taken offline by Vendor Monitor. | |
| offline_end_at | `TIMESTAMP` | Timestamp in Local when the vendor become reachable and available to receive orders again. | |
| offline_duration | `INTEGER` | The time interval between the `offline_start_at` and `offline_end_at` in seconds. | |

### Examples

#### Connectivity granular data in UTC time
```sql
SELECT c.entity_id
  , c.vendor_id
  , s.*
FROM cl.rps_connectivity c
LEFT JOIN UNNEST(slots_utc) s
```

#### Connectivity data per vendor by day in Local time
```sql
SELECT DATE(s.starts_at) AS created_date
  , c.entity_id
  , c.vendor_code
  , AVG(s.daily_schedule_duration) AS daily_schedule_duration
  , SUM(s.connectivity.unreachable_duration) AS unreachable_duration
FROM cl.rps_connectivity c
LEFT JOIN UNNEST(slots_local) s
GROUP BY 1,2,3
ORDER BY 1,2,3
```
