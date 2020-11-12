# Rider Special Payments

This table aggregates all rider special payments from rooster. Currently, it contains the csv imports.

| Column | Type | Description |
| :--- | :--- | :--- |
| region | `STRING`| The two-letter code of the operational region in which the country is located. The three operational regions are America (US), Europe and Middle East (EU), Asia (AP). |
| country_code | `STRING`| A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2. |
| created_date | `DATE`| Date of when the payment is created in Rooster. |
| rider_id | `INTEGER`| Identifier of the paid rider used by Rooster. |
| rider_name | `STRING`| Rider's name and surname used by Rooster. |
| timezone | `STRING`| The name of the timezone where the city is located. The timezone enables time conversion, from UTC to local time. |
| total_payment_date | `FLOAT`| The total of all payments in local currency within the date. |
| [payment details](#payment-details) | `<ARRAY>RECORD` | Payment details record. |

## Payment Details

| Column | Type | Description |
| :--- | :--- | :--- |
| payment_id | `INTEGER` | Identifier of the payment used by Rooster. |
| reason | `STRING` | Open text field to enter the reason of the special payment. |
| created_by | `STRING` | Email address of the user who created the payment. |
| created_at| `TIMESTAMP`| When the payment record is created in Rooster. |
| total | `FLOAT` | The total of the payments of that payment_id. |
| note | `STRING` | The annotation to the payment entered by the user. |
| payment_cycle_id | `INTEGER` | Running id system for the payment cycles. |
| payment_cycle_start_date | `TIMESTAMP` | Datetime when payment cycle starts. |
| payment_cycle_end_date | `TIMESTAMP` | Datetime when payment cycle ends. |
| status | `STRING` | Defines if a special payment is active or inactive, inactive special payments are not shown to riders. |
| adjustment_type | `STRING` | The type of the special payment. It can for example be related to `TAX`, `FEE`, `COMPLIANCE`, COD`, `OTHERS`. |
