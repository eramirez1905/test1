CREATE OR REPLACE TABLE `{{ params.project_id }}.cl_mkt.{{ params.table_name }}`
PARTITION BY DATE_TRUNC(order_date, MONTH)
CLUSTER BY entity_code
AS
SELECT countries.dwh_source_code as entity_code
    , orders.* EXCEPT (
        row_hash,
        merge_layer_run_from,
        merge_layer_created_at,
        merge_layer_updated_at
    )
FROM `dl_mkt.dwh_bl_marketing_orders` orders
LEFT JOIN `dl_mkt.dwh_il_dim_countries` countries USING (source_id)
