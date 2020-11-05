/*
There is no function in Redshift to get the polygon centroid, then we can calculate the centroid
of the polygon envelope (rectangle/square), by exposing it to further queries.
*/
CREATE TEMPORARY TABLE restaurants_polygons_envelopes AS
SELECT
    dra.source_id,
    dra.restaurant_id,
    ST_AsEWKT(ST_GeomFromText(dsp.sales_area_polygon_wkt)) AS polygon,
    -- calculate the envelope of the polygon, to make centroid calculations easier.
    ST_ENVELOPE(ST_GeomFromText(dsp.sales_area_polygon_wkt)) AS envelope
FROM {{ il_schema }}.dim_restaurant_area dra
INNER JOIN ds_location.dim_sales_polygon_{{ brand_code }} dsp
    ON dra.area_id = dsp.sales_area_id
        AND dra.source_id = dsp.source_id
;


/*
A virtual cluster is created to speedup subsequent joins between polygons and coordinates.
By truncating a coordinate up to the first decimal place we can get a precision of around 11 km.
Calculating the cluster_id based on the envelope's centroid latitude give us common locations with
a lower precision, if compared with the coordinates that we have, but on the other hand will reduce
the number of point to polygon comparison to its cluster area.
*/
CREATE TEMPORARY TABLE virtual_cluster_mapping AS
WITH polygon_centroid AS
(
    SELECT
       restaurant_id,
       source_id,
        -- Calculate the latitude of the envelope's centroid.
       (ST_YMAX(envelope) + ST_YMIN(envelope)) / 2 AS lat_centroid,
       polygon
    FROM restaurants_polygons_envelopes
)
SELECT DISTINCT
    source_id,
    restaurant_id,
    -- Truncate the centroid latitude to have a 11km cluster latitude area.
    trunc(lat_centroid, 1) * 100 AS cluster_id,
    polygon
FROM polygon_centroid
;


INSERT INTO {{ vendor_user_mapping_schema }}.{{ restaurants_polygons_table_name }} (
    source_id,
    restaurant_id,
    cluster_id,
    polygon
)
SELECT
    source_id,
    restaurant_id,
    cluster_id,
    polygon
FROM virtual_cluster_mapping
;

SET analyze_threshold_percent TO 0;
ANALYZE {{ vendor_user_mapping_schema }}.{{ restaurants_polygons_table_name }};
