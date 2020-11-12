# Table Last Updated

This table contains the last updated time for curated data tables

| Column | Type | Description |
| :--- | :--- | :--- |
| table_name | `STRING`| Name of the table. |
| last_updated | `TIMESTAMP`| When the table was last updated in UTC. |
| execution_date | `TIMESTAMP`| When the DAG, which generates table_name, started to run in UTC. |
| max_raw | `TIMESTAMP`| Up until which time, in UTC, the raw layer has data when the dag started to run (execution_date). |

#### Example for last updated for orders:

```sql
SELECT last_updated
FROM `fulfillment-dwh-production.curated_data_shared.table_last_updated`
WHERE table_name = 'orders'
```
