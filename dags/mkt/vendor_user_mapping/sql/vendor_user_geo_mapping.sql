/*
Populate the vendor user map with the information from the restaurants polygons and
customers that have their coordinates within these polygons. By using the polygon_point_map
as a dimension table we reduce the number of spatial comparisons and benefit from the
hash created by exchanging Memory for CPU.
*/
INSERT INTO {{ vendor_user_mapping_schema }}.{{ vendor_user_geo_mapping_table_name }} (
    source_id,
    restaurant_id,
    customer_id
)
WITH matched_coordinates_polygons AS (
    SELECT
        rp.source_id,
        rp.restaurant_id,
        co.customer_id,
        ROW_NUMBER() OVER (
            PARTITION BY rp.source_id, rp.restaurant_id, co.customer_id
        ) AS row_number
    FROM {{ vendor_user_mapping_schema }}.{{ customers_orders_coordinates_table_name }} co
    INNER JOIN {{ vendor_user_mapping_schema }}.{{ polygon_point_map_table_name }} ppm
        ON md5(co.coordinate) = ppm.coordinate
            AND co.source_id = ppm.source_id
    INNER JOIN {{ vendor_user_mapping_schema }}.{{ restaurants_polygons_table_name }} rp
        ON md5(rp.polygon) = ppm.polygon
            AND rp.source_id = ppm.source_id
)
SELECT
    source_id,
    restaurant_id,
    customer_id
FROM matched_coordinates_polygons
WHERE
    row_number = 1
;

SET analyze_threshold_percent TO 0;
ANALYZE {{ vendor_user_mapping_schema }}.{{ vendor_user_geo_mapping_table_name }};
