# lg_daily_rider_payments

Rider payments aggregated by rider and date

Partitioned by field `created_date_utc` with each
partition being a time interval of `DAY`

Clustered by fields `rdbms_id`, `lg_rider_uuid`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid represents an aggregation of rider payments in a date. So this should not be used for any JOIN as it is not the payment_id |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| lg_rider_uuid | `STRING` | Each uuid represents a rider in logistics |
| lg_rider_id | `INTEGER` |  |
| rider_name | `STRING` |  |
| country_code | `STRING` |  |
| timezone | `STRING` |  |
| exchange_rate | `FLOAT` |  |
| total_payment_eur | `NUMERIC` |  |
| total_payment_local | `NUMERIC` |  |
| created_date_utc | `DATE` |  |
| [details](#details) | `RECORD` |  |

## details

| Name | Type | Description |
| :--- | :--- | :---        |
| [basic](#basic) | `ARRAY<RECORD>` |  |
| [basic_by_rule](#basicbyrule) | `ARRAY<RECORD>` |  |
| [hidden_basic](#hiddenbasic) | `ARRAY<RECORD>` |  |
| [hidden_basic_by_rule](#hiddenbasicbyrule) | `ARRAY<RECORD>` |  |
| [quest](#quest) | `ARRAY<RECORD>` |  |
| [quest_by_rule](#questbyrule) | `ARRAY<RECORD>` |  |
| [scoring](#scoring) | `ARRAY<RECORD>` |  |

## basic

| Name | Type | Description |
| :--- | :--- | :---        |
| lg_payments_basic_rule_uuid | `STRING` |  |
| lg_payment_id | `INTEGER` |  |
| lg_payments_basic_rule_id | `INTEGER` |  |
| lg_delivery_id | `INTEGER` |  |
| lg_shift_id | `INTEGER` |  |
| lg_payment_cycle_id | `INTEGER` |  |
| status | `STRING` |  |
| payment_unit_type | `STRING` |  |
| payment_rule_name | `STRING` |  |
| city_name | `STRING` |  |
| total_local | `NUMERIC` |  |
| total_eur | `NUMERIC` |  |
| payment_unit_count | `FLOAT` |  |
| created_at_utc | `TIMESTAMP` |  |
| payment_cycle_start_at_utc | `TIMESTAMP` |  |
| payment_cycle_end_at_utc | `TIMESTAMP` |  |

## basic_by_rule

| Name | Type | Description |
| :--- | :--- | :---        |
| lg_payments_basic_rule_uuid | `STRING` |  |
| lg_payments_basic_rule_id | `INTEGER` |  |
| total_local | `NUMERIC` |  |
| total_eur | `NUMERIC` |  |
| payment_unit_count | `FLOAT` |  |

## hidden_basic

| Name | Type | Description |
| :--- | :--- | :---        |
| lg_payment_id | `INTEGER` |  |
| lg_payments_basic_rule_uuid | `STRING` |  |
| lg_payments_basic_rule_id | `INTEGER` |  |
| lg_delivery_id | `INTEGER` |  |
| lg_shift_id | `INTEGER` |  |
| lg_payment_cycle_id | `INTEGER` |  |
| status | `STRING` |  |
| payment_unit_type | `STRING` |  |
| payment_rule_name | `STRING` |  |
| city_name | `STRING` |  |
| total_local | `NUMERIC` |  |
| total_eur | `NUMERIC` |  |
| payment_unit_count | `FLOAT` |  |
| created_at_utc | `TIMESTAMP` |  |
| payment_cycle_start_at_utc | `TIMESTAMP` |  |
| payment_cycle_end_at_utc | `TIMESTAMP` |  |

## hidden_basic_by_rule

| Name | Type | Description |
| :--- | :--- | :---        |
| lg_payments_basic_rule_uuid | `STRING` |  |
| lg_payments_basic_rule_id | `INTEGER` |  |
| total_local | `NUMERIC` |  |
| total_eur | `NUMERIC` |  |
| payment_unit_count | `FLOAT` |  |

## quest

| Name | Type | Description |
| :--- | :--- | :---        |
| lg_payment_id | `INTEGER` |  |
| lg_payments_quest_rule_uuid | `STRING` |  |
| lg_payments_quest_rule_id | `INTEGER` |  |
| lg_goal_id | `INTEGER` |  |
| lg_payment_cycle_id | `INTEGER` |  |
| status | `STRING` |  |
| city_name | `STRING` |  |
| goal_type | `STRING` |  |
| payment_unit_type | `STRING` |  |
| payment_unit_count | `FLOAT` |  |
| payment_rule_name | `STRING` |  |
| duration_in_iso8601 | `STRING` |  |
| total_local | `NUMERIC` |  |
| total_eur | `NUMERIC` |  |
| payment_unit_threshold | `INTEGER` |  |
| deliveries_accepted_count | `INTEGER` |  |
| deliveries_notified_count | `INTEGER` |  |
| no_show_count | `INTEGER` |  |
| created_at_utc | `TIMESTAMP` |  |
| paid_period_start_utc | `TIMESTAMP` |  |
| paid_period_end_utc | `TIMESTAMP` |  |
| payment_cycle_start_at_utc | `TIMESTAMP` |  |
| payment_cycle_end_at_utc | `TIMESTAMP` |  |

## quest_by_rule

| Name | Type | Description |
| :--- | :--- | :---        |
| lg_payments_quest_rule_uuid | `STRING` |  |
| lg_payments_quest_rule_id | `INTEGER` |  |
| total_local | `NUMERIC` |  |
| total_eur | `NUMERIC` |  |
| payment_unit_count | `FLOAT` |  |

## scoring

| Name | Type | Description |
| :--- | :--- | :---        |
| lg_payment_id | `INTEGER` |  |
| lg_payments_scoring_rule_uuid | `STRING` |  |
| lg_payments_scoring_rule_id | `INTEGER` |  |
| lg_goal_id | `INTEGER` |  |
| lg_payment_cycle_id | `INTEGER` |  |
| lg_delivery_id | `INTEGER` |  |
| lg_evaluation_id | `INTEGER` |  |
| status | `STRING` |  |
| city_name | `STRING` |  |
| scoring_amount | `FLOAT` |  |
| goal_type | `STRING` |  |
| payment_unit_type | `STRING` |  |
| payment_unit_count | `FLOAT` |  |
| total_local | `NUMERIC` |  |
| total_eur | `NUMERIC` |  |
| created_at_utc | `TIMESTAMP` |  |
| paid_period_start_utc | `TIMESTAMP` |  |
| paid_period_end_utc | `TIMESTAMP` |  |
| payment_cycle_start_at_utc | `TIMESTAMP` |  |
| payment_cycle_end_at_utc | `TIMESTAMP` |  |
