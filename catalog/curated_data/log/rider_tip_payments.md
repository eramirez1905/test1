# Rider Tip Payments

This table aggregates all rider tip payments from rooster.

| Column | Type | Description |
| :--- | :--- | :--- |
| region | `STRING`| The two-letter code of the operational region in which the country is located. The three operational regions are America (US), Europe and Middle East (EU), Asia (AP). |
| country_code | `STRING`| A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2. |
| created_date | `DATE`| Date of when the payment is created in Rooster. |
| rider_id | `INTEGER`| Identifier of the paid rider used by Rooster. |
| rider_name | `STRING`| Rider's name and surname used by Rooster. |
| timezone | `STRING`| The name of the timezone where the city is located. The timezone enables time conversion, from UTC to local time. |
| total_payment_date | `FLOAT`| The total of all **tip** payments in local currency within the date. |
| [payment details](#payment-details) | `<ARRAY>RECORD` | Payment details record. |

## Payment Details

| Column | Type | Description |
| :--- | :--- | :--- |
| payment_id | `INTEGER` | Identifier of the payment used by Rooster. |
| created_at| `TIMESTAMP`| When the payment record is created in Rooster. |
| order_id | `INTEGER`| The identifier of the order generated within the logistics system. It is specific to the order within a country. |
| delivery_id | `INTEGER`| The identifier of the delivery generated within the logistics system. It is specific to the order within a country. |
| order_tip | `FLOAT` | The total of the tip paid in that order_id. |
| total | `FLOAT` | The total the rider received from that delivery. |
| payment_cycle_id | `INTEGER` | Running id system for the payment cycles. |
| payment_cycle_start_date | `TIMESTAMP` | Datetime when payment cycle starts. |
| payment_cycle_end_date | `TIMESTAMP` | Datetime when payment cycle ends. |
| status| `STRING`| Defines if a special payment is active or inactive, inactive tip payments are not shown to riders. |
