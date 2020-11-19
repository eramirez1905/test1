# Rider Deposit Payments

This table aggregates all rider deposit payments from rooster.

| Column | Type | Description |
| :--- | :--- | :--- |
| region | `STRING`| The two-letter code of the operational region in which the country is located. The three operational regions are America (US), Europe and Middle East (EU), Asia (AP). |
| country_code | `STRING`| A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2. |
| created_date | `DATE`| Date of when the payment is created in Rooster. **This date is in local time** |
| rider_id | `INTEGER`| Identifier of the paid rider used by Rooster. |
| rider_name | `STRING`| Rider's name and surname used by Rooster. |
| timezone | `STRING`| The name of the timezone where the city is located. The timezone enables time conversion, from UTC to local time. |
| paid_amount | `FLOAT`| The total payment in local currency that rider has already paid. |
| remaining_amount | `FLOAT`| The total payment in local currency that rider still has to pay. |
| status| `STRING`| Defines if a deposit payment is active or inactive, inactive special payments are not shown to riders. |
| payment_cycle_id | `INTEGER` | Running id system for the payment cycles. |
| payment_cycle_start_date | `TIMESTAMP` | Datetime when payment cycle starts. |
| payment_cycle_end_date | `TIMESTAMP` | Datetime when payment cycle ends. |
| payment_rule_id | `INTEGER` | Identifier of the payment rule used by Rooster. |
| created_at| `TIMESTAMP`| When the payment record is created in Rooster. |
| updated_at| `TIMESTAMP`| When the payment record is updated in Rooster. |

