# Feature Store

## Backfill

- The query `live_orders_backfill.sql` is used both for the backfill and daily run.

## Daily

- Queued orders uses the `feature_store_real_time_view` to create its daily view. 
Notice the `if` condition in `feature_store_real_time_view.sql` 

- The `feature_store_real_time_view.sql` file should contain the logic to get:
  - Queued Orders
  - Live Orders
  - Delay Orders
  - Shrinking information
 
 - The idea of the previous is that it is read once from DataFridge tables.
