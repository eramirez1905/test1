/*
The Point to Polygon calculation uses unique polygons and coordinates to find a match between them.
Matching every coordinate to every polygon may lead to unnecessary computation, for example
when you have many restaurants sharing the same delivery area polygon, there is no need to calculate
the match for every duplicated polygon.
*/
CREATE TEMPORARY TABLE unique_polygons AS
WITH polygons AS
(
    SELECT DISTINCT
        polygon,
        source_id,
        cluster_id
    FROM {{ vendor_user_mapping_schema }}.{{ restaurants_polygons_table_name }}
)
SELECT
    ST_GeomFromText(polygon) AS polygon,
    source_id,
    cluster_id
FROM polygons
;


CREATE TEMPORARY TABLE unique_coordinates AS
WITH customers_coordinates AS
(
    SELECT DISTINCT
        coordinate,
        source_id,
        cluster_id
    FROM {{ vendor_user_mapping_schema }}.{{ customers_orders_coordinates_table_name }}
)
SELECT
    ST_GeomFromText(coordinate) AS coordinate,
    source_id,
    cluster_id
FROM customers_coordinates
;


/*
To make later calculations faster and reduce the size of the map table, a hash function
is applied to the polygon and coordinate columns.
*/
INSERT INTO {{ vendor_user_mapping_schema }}.{{ polygon_point_map_table_name }} (
    source_id,
    cluster_id,
    polygon,
    coordinate
)
SELECT
    rp.source_id,
    co.cluster_id,
    md5(ST_AsEWKT(rp.polygon)),
    md5(ST_AsEWKT(co.coordinate))
FROM unique_coordinates co
INNER JOIN unique_polygons rp
    ON co.source_id = rp.source_id
        AND co.cluster_id = rp.cluster_id
        AND ST_CONTAINS(rp.polygon, co.coordinate)
;

SET analyze_threshold_percent TO 0;
ANALYZE {{ vendor_user_mapping_schema }}.{{ brand_code }}_polygon_point_map;
