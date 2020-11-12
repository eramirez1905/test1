# Rider Wallet

This table lists the cash transactions that happened within the rider's wallet. 

| Column | Type | Description |
| :--- | :--- | :--- |
| country_code | `STRING`| A two-character alphanumeric code based on the code of the country as specified by ISO 3166-1 ALPHA-2. |
| transaction_id | `INTEGER` | ID identifying the transaction within the rider wallet. |
| rider_id | `INTEGER`| Identifier of the rider used by Rooster |
| delivery_id | `INTEGER`| ID identifying the delivery. |
| order_id | `INTEGER` | ID identifying the order associated to the delivery. |
| timezone| `STRING` | The name of the timezone where the city is located. The timezone enables time conversion, from UTC to local time. |
| city_id | `INTEGER` | ID identifying the city within the logistics systems. |
| created_at | `TIMESTAMP` | The date and time when the transaction was created. |
| updated_at | `TIMESTAMP` | The date and time when the transaction was updated. |
| type | `STRING` | The type of the transaction: <br>`CASH_IN`: Rider received cash, typically at customer or at beginning of shift <br>`CASH_OUT`: Rider gave cash, typically at a restaurant or at end of shift <br>`ADJUSTMENT`: Adjustment made by dispatcher, resolving issues or discrepancies <br>`COLLECTION`: Rider collected money at dropoff <br>`PAYMENT`: Rider payed money at pickup <br>`REJECTED_DISCREPANCY`: Issue reviewed and declined, so the difference is inserted in the courier journal <br>`COURIER_COMMISSION`: Rider commission fees. This entry type will be superseded by the `COURIER_PAYMENT one` <br>`COURIER_PAYMENT`: Rider payment calculated in Payments Service <br>`COURIER_QUEST_PAYMENT`: Rider quest payment calculated in Payments Service <br>`ACCEPTED_DISCREPANCY`: Issue reviewed and accepted, so the difference is inserted in the courier journal <br> `AUTOMATIC_REJECTED_DISCREPANCY`: Issue automatically rejected, so the difference is inserted in the courier journal.|
| amount | `INTEGER` | The monetary value, as indicated by the rider, that was deducted or added to the rider's wallet for this transaction. |
| balance | `INTEGER` | The wallet balance the rider sees in his/her app after that transaction. |
| created_by | `STRING` | Showing who created the transaction in the wallet. If it is done by the rider it will state `courier`, otherwise the email of the user who did it manually. |
| manual | `BOOLEAN` | Flag that indicates whether the transaction was done manually or automatically. |
| mass_update | `BOOLEAN` | Flag that indicates whether the transaction was created via bulk upload. |
| note | `STRING` | Annotation providing any furher explanatory comments on a journal entry. |
| external_provider_id | `STRING` | Actual payment provider id. It is usually a bank like HSBC/STCPAY. |
| integrator_id | `STRING` | Integrator Id. COD use an integrator for payment transactions like ALFRED/IXOPAY. |
| status | `STRING` | The current status of the transaction. It could be `PENDING`, `SUCCESS`, `REJECTED`, `ERROR`, `EXPIRED` depending on the payment service provider. |
| message | `STRING` | An optional description why the transaction is in the current status. |
| [issues](#issues) | `ARRAY<RECORD>` | N\A |

## Issues

Issues can occur when the rider selects that he/she either paid less money to the vendor or collected less money from the customer. All issues are reviewed manually.

| Column | Type | Description |
| :--- | :--- | :--- |
| issue_id | `INTEGER` | Identifier of the issue. |
| orignal_amount | `INTEGER` | The original amount of the transaction. This will deviate to the transaction amount in case the rider made an adjustment. |
| justification | `STRING` | The justification for the adjustment selected by the rider. |
| created_by | `STRING` | Showing who created the issue. |
| created_at | `TIMESTAMP` | The time and date the issue was created. |
| updated_at | `TIMESTAMP` | The time and date the issue was updated. |
| updated_by | `STRING` | Showing who updated the issue. |
| reviewed | `BOOLEAN` | Flag indicating whether the issue has been reviewed manually. |
| approved | `BOOLEAN` | Flag indicating whether the issue has been approved manually. |
