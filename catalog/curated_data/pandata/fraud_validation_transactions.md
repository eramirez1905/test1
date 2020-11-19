# fraud_validation_transactions

Table of fraud validation transactions with each row representing one fraud validation transaction

Partitioned by field `created_date_utc` with each
partition being a time interval of `DAY`

Clustered by field `rdbms_id`


| Name | Type | Description |
| :--- | :--- | :---        |
| uuid | `STRING` | Each uuid is unique and represents a fraud validation transaction |
| id | `INTEGER` |  |
| rdbms_id | `INTEGER` | Each country has one id and this table is clustered by this field |
| score | `FLOAT` |  |
| validator_code | `STRING` |  |
| order_code | `STRING` |  |
| is_success | `BOOLEAN` |  |
| is_error | `BOOLEAN` |  |
| is_bad | `BOOLEAN` |  |
| transaction_reference | `STRING` |  |
| handler_data | `STRING` |  |
| created_at_local | `TIMESTAMP` |  |
| created_at_utc | `TIMESTAMP` |  |
| created_date_local | `DATE` |  |
| created_date_utc | `DATE` |  |
