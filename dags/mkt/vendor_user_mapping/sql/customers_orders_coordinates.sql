/*
 Populate the {{ customers_orders_coordinates_table_name }} table with the last customer's
 location based on the order location for the past {{ vendor_user_mapping_lookback_months }}
 number of months.
 */
INSERT INTO {{ vendor_user_mapping_schema }}.{{ customers_orders_coordinates_table_name }} (
    source_id,
    customer_id,
    last_order_date,
    coordinate,
    cluster_id,
    dwh_row_hash
)
WITH customer_orders_received AS
(
    SELECT
        source_id,
        analytical_customer_id AS customer_id,
        latitude,
        longitude,
        order_date,
        ROW_NUMBER() OVER (
            PARTITION BY source_id, analytical_customer_id
            ORDER BY order_date DESC
        ) AS row_number
    FROM {{ il_schema }}.ranked_fct_order
    WHERE
        order_date >= add_months(CURRENT_DATE, {{ vendor_user_mapping_lookback_months }})
        AND latitude IS NOT NULL
        AND longitude IS NOT NULL
        AND is_sent
)
SELECT
    source_id,
    customer_id,
    order_date,
    ST_AsEWKT(ST_MAKEPOINT(longitude, latitude)),
    trunc(latitude, 1) * 100 AS cluster_id,
    SHA1(source_id || customer_id) AS dwh_row_hash
FROM customer_orders_received
WHERE
    row_number = 1
;

SET analyze_threshold_percent TO 0;
ANALYZE {{ vendor_user_mapping_schema }}.{{ customers_orders_coordinates_table_name }};
