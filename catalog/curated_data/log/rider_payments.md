# Rider Payments

This table aggregates all rider payment types from rooster. 

| Column | Type | Description |
| :--- | :--- | :--- |
| region | `STRING`| The two-letter code of the operational region in which the country is located. The three operational regions are America (US), Europe and Middle East (EU), Asia (AP). |
| country_code | `STRING`| A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2. |
| created_date | `DATE`| Date of when the payment is created in Rooster. |
| rider_id | `INTEGER`| Identifier of the rider used by Rooster. |
| rider_name | `STRING`| Rider's name and surname used by Rooster. |
| timezone | `STRING`| The name of the timezone where the city is located. The timezone enables time conversion, from UTC to local time. |
| exchange_rate | `FLOAT`| The value of one currency for the purpose of conversion to another based on `EUR`. |
| total_date_payment_eur | `NUMERIC`| The total of all payments in euro (eg: basic, quest, scoring). |
| total_date_payment | `FLOAT`| The total of all payments in local currency (eg: basic, quest, scoring). |
| [payment details](#payment-details) | `RECORD` | Payment details record. |

## Payment details

| Column | Type | Description |
| :--- | :--- | :--- |
| [basic](#basic) | `ARRAY<RECORD>` | Basic payments array record. |
| [quest](#quest) | `ARRAY<RECORD>` | Quest payments array record. |
| [scoring](#scoring) | `ARRAY<RECORD>` | Scoring payments array record. |
| [hidden_basic](#hidden-basic) | `ARRAY<RECORD>` | Hidden basic payments array record. |

## Basic

| Column | Type | Description |
| :--- | :--- | :--- |
| payment_id | `INTEGER` | Identifier of the payment used by Rooster. |
| payment_rule_id | `INTEGER` | Identifier of the payment rule used by Rooster. |
| status| `STRING`| It indicates if the payment has been made to the rider. |
| delivery_id | `INTEGER` | Id identifying the delivery in hurrier, introduced on 2019-09-05, NULL for previous dates. |
| shift_id | `INTEGER` | Id of the shift in Rooster. | 
| payment_cycle_id | `INTEGER` | Running id system for the payment cycles. |
| payment_cycle_start_date | `TIMESTAMP` | Datetime when payment cycle starts. |
| payment_cycle_end_date | `TIMESTAMP` | Datetime when payment cycle ends. |
| payment_type| `STRING`| Type to which payment is referring to (EVALUATION, DELIVERY). |
| payment_rule_name| `STRING`| Name of the rule to which payment is referring to. |
| city_name| `STRING`| Name of the city where to which payment is referring to. |
| created_at| `TIMESTAMP`| When the payment record is created in Rooster. |
| total| `NUMERIC`| Total basic payments in local currency. |
| total_eur| `NUMERIC`| Total basic payments in euro. |
| amount| `FLOAT`| amount of units for payments in hours, deliveries, distance. |
| basic_by_rule| `ARRAY<RECORD>`| Basic payments array record containing subtotals by `payment_rule_id`. |

## Quest

| Column | Type | Description |
| :--- | :--- | :--- |
| created_at| `TIMESTAMP`| When the payment record is created in Rooster. |
| payment_rule_id | `INTEGER` | Identifier of the payment rule used by Rooster. |
| city_name| `STRING`| Name of the city where to which payment is referring to. ||
| goal_id| `INTEGER`|  Identifies (together with type quest) the thresholds for every quest payment on the Goal table in Rooster. |
| threshold | `INTEGER` | minimum amount of payment type units a rider has to reach, before he receives the bonus. |
| payment_cycle_id | `INTEGER` | Running id system for the payment cycles. |
| payment_cycle_start_date | `TIMESTAMP` | Datetime when payment cycle starts. |
| payment_cycle_end_date | `TIMESTAMP` | Datetime when payment cycle ends. |
| accepted_deliveries | `INTEGER` | Number of accepted deliveries within the payment date. |
| notified_deliveries | `INTEGER` | Number of notified deliveries within the payment date. |
| no_shows | `INTEGER` | Number of no-show shifts within the payment date. |
| paid_period_start | `TIMESTAMP` | Start of the period taking the payment into consideration.  | 
| paid_period_end | `TIMESTAMP` | End of the period taking the payment into consideration.  |
| total| `NUMERIC`| Total quest payments in local currency. |
| total_eur| `NUMERIC`| Total quest payments in euro. |
| status| `STRING`| defines if a quest payment is active or inactive, inactive quest payments are not shown to riders. |
| quest_payment_rule_name| `STRING`| Name of the rule to which payment is referring to. |
| total| `FLOAT`| Total quest payments in local currency. |
| duration| `STRING`| defines, how long one season of the quest payment takes. There can be multiple season between period start and end. |
| payment_id | `INTEGER` | Identifier of the payment used by Rooster. |
| payment_type| `STRING`| Type to which payment is referring to (per delivery, per hour, per km). |
| amount| `FLOAT`| Units from payment type, which result in a payment. |
| acceptance_rate| `FLOAT`| The acceptance rate calculated from rider stats service. Please note: this calculation may differ from `accepted_deliveries` / `notified_deliveries`, as in some markets would be calculated as (`accepted_deliveries` - `manual_undispatches`) / `notified_deliveries`. |
| quest_by_rule| `ARRAY<RECORD>`| Quest payments array record containing subtotals by `payment_rule_id`. |

## Scoring

| Column | Type | Description |
| :--- | :--- | :--- |
| created_at| `TIMESTAMP`| When the payment record is created in Rooster. |
| payment_rule_id | `INTEGER` | Identifier of the payment rule used by Rooster. |
| city_name| `STRING`| Name of the city where to which payment is referring to. |
| scoring_amount| `FLOAT`| Scoring batch number. |
| goal_id| `INTEGER`| Identifies (together with type scoring) the thresholds for every scoring payment on the Goal table in Rooster. |
| payment_cycle_id | `INTEGER` | Running id system for the payment cycles. |
| payment_cycle_start_date | `TIMESTAMP` | Datetime when payment cycle starts. |
| payment_cycle_end_date | `TIMESTAMP` | Datetime when payment cycle ends. |
| paid_period_start | `TIMESTAMP` | Start of the period taking the payment into consideration.  | 
| paid_period_end | `TIMESTAMP` | End of the period taking the payment into consideration.  |
| total| `NUMERIC`| Total scoring payments in local currency. |
| total_eur| `NUMERIC`| Total scoring payments in euro. |
| status| `STRING`| defines if a quest payment is active or inactive, inactive scoring payments are not shown to riders. |
| payment_id | `INTEGER` | Identifier of the payment used by Rooster. |
| payment_type| `STRING`| Type to which payment is referring to (batch). |
| goal_type| `STRING`| defines the payout type, can either be per unit or per goal, which is a one off payment. |
| paid_unit_amount| `FLOAT`| total amount of units used for payment, can be deliveries, hours, distance etc. |
| delivery_id | `INTEGER` | Id identifying the delivery in hurrier. `NULL` if payment is not `PER_DELIVERY` |
| evaluation_id| `INTEGER`| Id identifying the evaluation related to the payment.`NULL` if payment is not `PER_HOUR`. |

## Hidden Basic

Hidden basic payments are basic payments not shown to the riders. For example, they are used to create third party royalties

| Column | Type | Description |
| :--- | :--- | :--- |
| payment_id | `INTEGER` | Identifier of the payment used by Rooster. |
| payment_rule_id | `INTEGER` | Identifier of the payment rule used by Rooster. |
| status| `STRING`| It indicates if the payment has been made to the rider. |
| delivery_id | `INTEGER` | Id identifying the delivery in hurrier, introduced on 2019-09-05, NULL for previous dates. |
| shift_id | `INTEGER` | Id of the shift in Rooster. | 
| payment_cycle_id | `INTEGER` | Running id system for the payment cycles. |
| payment_cycle_start_date | `TIMESTAMP` | Datetime when payment cycle starts. |
| payment_cycle_end_date | `TIMESTAMP` | Datetime when payment cycle ends. |
| payment_type| `STRING`| Type to which payment is referring to (EVALUATION, DELIVERY). |
| payment_rule_name| `STRING`| Name of the rule to which payment is referring to. |
| city_name| `STRING`| Name of the city where to which payment is referring to. |
| created_at| `TIMESTAMP`| When the payment record is created in Rooster. |
| total| `NUMERIC`| Total basic payments in local currency. |
| total_eur| `NUMERIC`| Total basic payments in euro. |
| amount| `FLOAT`| amount of units for payments in hours, deliveries, distance. |
| hidden_basic_by_rule| `ARRAY<RECORD>`| Basic payments array record containing subtotals by `payment_rule_id`. |
